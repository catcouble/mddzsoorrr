"""测试获取用户资料 API"""
import requests

API_BASE = "http://localhost:8000"
API_KEY = "han1234"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def test_get_user_profile(username: str = "happyremixing"):
    """获取用户资料"""
    print("=" * 50)
    print(f"测试: 获取用户资料 (@{username})")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/profile/{username}",
        headers=headers
    )
    
    print(f"状态码: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if data.get("success"):
            profile = data.get("profile", {})
            print(f"\n用户信息:")
            print(f"  用户名: @{profile.get('username')}")
            print(f"  显示名: {profile.get('display_name')}")
            print(f"  粉丝数: {profile.get('follower_count')}")
            print(f"  关注数: {profile.get('following_count')}")
            print(f"  发布数: {profile.get('post_count')}")
            print(f"  获赞数: {profile.get('likes_received_count')}")
            print(f"  Remix数: {profile.get('remix_count')}")
            print(f"  Cameo数: {profile.get('cameo_count')}")
            print(f"  可Cameo: {profile.get('can_cameo')}")
            print(f"  认证: {profile.get('verified')}")
            print(f"  简介: {profile.get('description', 'N/A')[:100]}...")
            print(f"  链接: {profile.get('permalink')}")
    else:
        print(f"错误: {response.json()}")
    
    return response.json()

def test_user_not_found():
    """测试不存在的用户"""
    print("\n" + "=" * 50)
    print("测试: 请求不存在的用户")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/profile/this_user_does_not_exist_12345",
        headers=headers
    )
    
    print(f"状态码: {response.status_code}")
    print(f"响应: {response.json()}")

if __name__ == "__main__":
    test_get_user_profile("happyremixing")
    test_get_user_profile("wangdou")
    test_user_not_found()
