"""测试公共 Feed API"""
import requests

API_BASE = "http://localhost:8000"
API_KEY = "han1234"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def test_latest_feed():
    """获取最新 Feed"""
    print("=" * 50)
    print("测试: 获取最新 Feed (cut=nf2_latest)")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/feed",
        headers=headers,
        params={
            "limit": 5,
            "cut": "nf2_latest"
        }
    )
    
    print(f"状态码: {response.status_code}")
    data = response.json()
    print(f"成功: {data.get('success')}")
    print(f"数量: {data.get('count')}")
    print(f"游标: {data.get('cursor', 'N/A')[:50]}..." if data.get('cursor') else "游标: N/A")
    
    if data.get('items'):
        print("\n前3条内容:")
        for i, item in enumerate(data['items'][:3]):
            print(f"  {i+1}. ID: {item.get('id')}")
            print(f"     文本: {item.get('text', 'N/A')[:50]}...")
            print(f"     作者: @{item.get('author', {}).get('username', 'N/A')}")
            print(f"     点赞: {item.get('like_count', 0)}, 浏览: {item.get('view_count', 0)}")
    
    return data

def test_top_feed():
    """获取热门 Feed"""
    print("\n" + "=" * 50)
    print("测试: 获取热门 Feed (cut=nf2_top)")
    print("=" * 50)
    
    response = requests.get(
        f"{API_BASE}/api/feed",
        headers=headers,
        params={
            "limit": 5,
            "cut": "nf2_top"
        }
    )
    
    print(f"状态码: {response.status_code}")
    data = response.json()
    print(f"成功: {data.get('success')}")
    print(f"数量: {data.get('count')}")
    
    if data.get('items'):
        print("\n前3条热门内容:")
        for i, item in enumerate(data['items'][:3]):
            print(f"  {i+1}. ID: {item.get('id')}")
            print(f"     文本: {item.get('text', 'N/A')[:50]}...")
            print(f"     作者: @{item.get('author', {}).get('username', 'N/A')}")
            print(f"     点赞: {item.get('like_count', 0)}, 浏览: {item.get('view_count', 0)}")
    
    return data

def test_feed_pagination():
    """测试 Feed 分页"""
    print("\n" + "=" * 50)
    print("测试: Feed 分页")
    print("=" * 50)
    
    # 第一页
    response1 = requests.get(
        f"{API_BASE}/api/feed",
        headers=headers,
        params={"limit": 2, "cut": "nf2_latest"}
    )
    data1 = response1.json()
    cursor = data1.get('cursor')
    
    print(f"第一页数量: {data1.get('count')}")
    print(f"获取到游标: {'是' if cursor else '否'}")
    
    if cursor:
        # 第二页
        response2 = requests.get(
            f"{API_BASE}/api/feed",
            headers=headers,
            params={"limit": 2, "cut": "nf2_latest", "cursor": cursor}
        )
        data2 = response2.json()
        print(f"第二页数量: {data2.get('count')}")
        print(f"分页测试: {'成功' if data2.get('success') else '失败'}")

if __name__ == "__main__":
    test_latest_feed()
    test_top_feed()
    test_feed_pagination()
