"""测试获取用户发布内容 API"""
import requests

API_BASE = "http://localhost:8000"
API_KEY = "han1234"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def test_get_user_feed(user_id: str = "user-4qluo8ATzeEsuvCpOUAfAZY0"):
    """获取用户发布的内容"""
    print("=" * 50)
    print(f"测试: 获取用户发布内容 ({user_id})")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/user/{user_id}/feed",
        headers=headers,
        params={"limit": 5}
    )
    
    print(f"状态码: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if data.get("success"):
            feed = data.get("feed", {})
            items = feed.get("items", [])
            cursor = feed.get("cursor")
            
            print(f"获取到 {len(items)} 条内容")
            print(f"有下一页: {'是' if cursor else '否'}")
            
            for i, item in enumerate(items[:3]):
                post = item.get("post", {})
                profile = item.get("profile", {})
                attachments = post.get("attachments", [])
                
                print(f"\n  {i+1}. ID: {post.get('id')}")
                print(f"     文本: {post.get('text', 'N/A')[:50]}...")
                print(f"     点赞: {post.get('like_count', 0)}, 浏览: {post.get('view_count', 0)}, Remix: {post.get('remix_count', 0)}")
                
                if attachments:
                    att = attachments[0]
                    print(f"     尺寸: {att.get('width')}x{att.get('height')}")
                    print(f"     视频: {att.get('downloadable_url', 'N/A')[:80]}...")
            
            return data, cursor
    else:
        print(f"错误: {response.json()}")
    
    return None, None

def test_pagination():
    """测试分页"""
    print("\n" + "=" * 50)
    print("测试: 分页获取")
    print("=" * 50)
    
    # 第一页
    data, cursor = test_get_user_feed()
    
    if cursor:
        print(f"\n获取第二页...")
        response = requests.get(
            f"{API_BASE}/api/user/user-4qluo8ATzeEsuvCpOUAfAZY0/feed",
            headers=headers,
            params={"limit": 5, "cursor": cursor}
        )
        
        if response.status_code == 200:
            data2 = response.json()
            items = data2.get("feed", {}).get("items", [])
            print(f"第二页获取到 {len(items)} 条内容")

def test_with_profile():
    """先获取用户资料，再获取其发布内容"""
    print("\n" + "=" * 50)
    print("测试: 通过用户名获取资料后获取发布内容")
    print("=" * 50)
    
    # 1. 获取用户资料
    response = requests.get(
        f"{API_BASE}/api/profile/happyremixing",
        headers=headers
    )
    
    if response.status_code == 200:
        profile = response.json().get("profile", {})
        user_id = profile.get("user_id")
        username = profile.get("username")
        print(f"用户: @{username} ({user_id})")
        
        # 2. 获取用户发布内容
        if user_id:
            test_get_user_feed(user_id)

if __name__ == "__main__":
    test_get_user_feed()
    test_pagination()
    test_with_profile()
