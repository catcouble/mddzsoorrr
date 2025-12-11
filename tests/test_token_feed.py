"""测试 Token 发布内容 API"""
import requests

API_BASE = "http://localhost:8000"
API_KEY = "han1234"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def test_token_profile_feed(token_id: int = 1):
    """获取指定 Token 的发布内容"""
    print("=" * 50)
    print(f"测试: 获取 Token {token_id} 的发布内容")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/tokens/{token_id}/profile-feed",
        headers=headers,
        params={"limit": 12}
    )
    
    print(f"状态码: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"成功: {data.get('success')}")
        print(f"Token ID: {data.get('token_id')}")
        
        feed = data.get('feed', {})
        items = feed.get('items', [])
        print(f"内容数量: {len(items)}")
        
        if items:
            print("\n发布内容列表:")
            for i, item in enumerate(items[:5]):
                post = item.get('post', {})
                profile = item.get('profile', {})
                attachments = post.get('attachments', [])
                
                print(f"\n  {i+1}. ID: {post.get('id')}")
                print(f"     文本: {post.get('text', 'N/A')[:50]}..." if post.get('text') else "     文本: N/A")
                print(f"     用户: @{profile.get('username', 'N/A')}")
                
                if attachments:
                    att = attachments[0]
                    print(f"     类型: {att.get('kind', 'N/A')}")
                    print(f"     尺寸: {att.get('width', 'N/A')}x{att.get('height', 'N/A')}")
                    if att.get('downloadable_url'):
                        print(f"     下载链接: {att.get('downloadable_url')[:80]}...")
    else:
        print(f"错误: {response.json()}")
    
    return response.json()

def test_token_not_found():
    """测试不存在的 Token"""
    print("\n" + "=" * 50)
    print("测试: 请求不存在的 Token")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/tokens/99999/profile-feed",
        headers=headers
    )
    
    print(f"状态码: {response.status_code}")
    print(f"响应: {response.json()}")

if __name__ == "__main__":
    test_token_profile_feed(1)
    test_token_not_found()
