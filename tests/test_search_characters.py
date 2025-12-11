"""测试搜索角色 API"""
import requests

API_BASE = "http://localhost:8000"
API_KEY = "han1234"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def test_search_users():
    """搜索所有用户"""
    print("=" * 50)
    print("测试: 搜索所有用户 (intent=users)")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/characters/search",
        headers=headers,
        params={
            "username": "eliusertest",
            "intent": "users",
            "limit": 5
        }
    )
    
    print(f"状态码: {response.status_code}")
    print(f"响应: {response.json()}")
    return response.json()

def test_search_cameo():
    """搜索可用于视频生成的角色"""
    print("\n" + "=" * 50)
    print("测试: 搜索 Cameo 角色 (intent=cameo)")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/characters/search",
        headers=headers,
        params={
            "username": "eliusertest",
            "intent": "cameo",
            "limit": 5
        }
    )
    
    print(f"状态码: {response.status_code}")
    print(f"响应: {response.json()}")
    return response.json()

if __name__ == "__main__":
    test_search_users()
    test_search_cameo()
