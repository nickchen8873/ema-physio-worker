import os
import time
import base64
import requests
import re
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from supabase import create_client, Client

# 載入環境變數
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
FITBIT_CLIENT_ID = os.environ.get("FITBIT_CLIENT_ID")
FITBIT_CLIENT_SECRET = os.environ.get("FITBIT_CLIENT_SECRET")

# 初始化 Supabase 用戶端
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def refresh_fitbit_token(participant_id, old_refresh_token):
    """向 Fitbit 請求換發新的 Token"""
    url = "https://api.fitbit.com/oauth2/token"
    
    # Fitbit 要求的 Basic Auth 格式
    auth_str = base64.b64encode(f"{FITBIT_CLIENT_ID}:{FITBIT_CLIENT_SECRET}".encode('utf-8')).decode('utf-8')
    headers = {
        "Authorization": f"Basic {auth_str}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": old_refresh_token
    }

    print(f"[{participant_id}] 正在嘗試刷新 Token...")
    res = requests.post(url, headers=headers, data=data)
    
    if res.status_code == 200:
        new_token_data = res.json()
        
        # 計算新的過期時間(改用 UTC)
        expires_in = new_token_data.get("expires_in", 28800)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        # 寫回 Supabase
        update_data = {
            "fitbit_access_token": new_token_data["access_token"],
            "fitbit_refresh_token": new_token_data["refresh_token"],
            "expires_at": expires_at.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat() # 這裡也改用 UTC
        }
        supabase.table("user_tokens").update(update_data).eq("participant_id", participant_id).execute()
        
        print(f"[{participant_id}] Token 刷新成功！")
        return new_token_data["access_token"]
    else:
        print(f"[{participant_id}] ❌ Token 刷新失敗: {res.text}")
        return None

def fetch_and_store_fetch_data(target_date=None):
    # 如果有傳入指定日期就用指定的，沒有就預設抓昨天
    fetch_date = target_date if target_date else (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"=== 開始抓取 {fetch_date} 的生理資料 ===")

    # 從資料庫撈出所有使用者
    response = supabase.table("user_tokens").select("*").execute()
    users = response.data

    if not users:
        print("目前資料庫中沒有任何已綁定的受試者。")
        return

    for user in users:
        p_id = user['participant_id']
        access_token = user['fitbit_access_token']
        refresh_token = user['fitbit_refresh_token']
        
        # 簡單檢查 Token 是否已經過期 (保留 5 分鐘的容錯空間)
        # 用 re.sub 把小數點和後面的數字強制清空
        clean_time_str = re.sub(r'\.\d+', '', user['expires_at'].replace('Z', '+00:00'))
        expires_at = datetime.fromisoformat(clean_time_str)
        
        if datetime.now(timezone.utc) > (expires_at - timedelta(minutes=5)):
            access_token = refresh_fitbit_token(p_id, refresh_token)
            if not access_token:
                continue # 如果刷新失敗，跳過這個受試者，處理下一個

        headers = {"Authorization": f"Bearer {access_token}"}
        
        try:
            # 1. 抓取活動數據 (包含久坐、輕度、MVPA)
            act_res = requests.get(f"https://api.fitbit.com/1/user/-/activities/date/{fetch_date}.json", headers=headers)
            
            # 如果還是遇到 401，可能是剛剛沒檢查到，再強制刷新一次
            if act_res.status_code == 401:
                access_token = refresh_fitbit_token(p_id, refresh_token)
                if not access_token: continue
                headers = {"Authorization": f"Bearer {access_token}"}
                act_res = requests.get(f"https://api.fitbit.com/1/user/-/activities/date/{fetch_date}.json", headers=headers)

            act_data = act_res.json().get('summary', {})
            sedentary = act_data.get('sedentaryMinutes', 0)
            light = act_data.get('lightlyActiveMinutes', 0)
            mvpa = act_data.get('fairlyActiveMinutes', 0) + act_data.get('veryActiveMinutes', 0)

            # 2. 抓取睡眠數據
            sleep_res = requests.get(f"https://api.fitbit.com/1.2/user/-/sleep/date/{fetch_date}.json", headers=headers).json()
            sleep_records = sleep_res.get('sleep', [])

            # 初始化數值
            sleep_hours = None
            sleep_efficiency = None

            if sleep_records:
                # 策略：尋找當天的「主睡眠」，如果沒標記則取第一筆
                main_sleep = next((s for s in sleep_records if s.get('isMainSleep')), sleep_records[0])
                
                # 抓取分鐘數並轉換
                minutes_asleep = main_sleep.get('minutesAsleep') or main_sleep.get('totalMinutesAsleep') or 0
                sleep_hours = round(minutes_asleep / 60, 2)
                sleep_efficiency = main_sleep.get('efficiency')

            # 3. 抓取 HRV 數據
            hrv_res = requests.get(f"https://api.fitbit.com/1/user/-/hrv/date/{fetch_date}.json", headers=headers).json()
            hrv_records = hrv_res.get('hrv', [])
            hrv_average = hrv_records[0].get('value', {}).get('dailyRmssd') if len(hrv_records) > 0 else None

            # 4. 組裝並 Upsert 到 Supabase
            payload = {
                "participant_id": p_id,
                "record_date": fetch_date,
                "sedentary_minutes": sedentary,
                "light_activity_minutes": light,
                "mvpa_minutes": mvpa,
                "sleep_hours": sleep_hours,
                "sleep_efficiency": sleep_efficiency,
                "hrv_average": hrv_average
            }

            supabase.table("daily_physiology").upsert(payload, returning="minimal").execute()
            print(f"[{p_id}] ✅ 資料更新成功")

        except Exception as e:
            print(f"[{p_id}] ❌ 發生預期外錯誤: {str(e)}")

        # 為了避免觸發 Fitbit 的每小時 150 次 Rate Limit，稍微暫停一下
        time.sleep(1)

    print("=== 任務執行完畢 ===")

if __name__ == "__main__":
    fetch_and_store_fetch_data()