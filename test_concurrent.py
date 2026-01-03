#!/usr/bin/env python3
"""
并发测试脚本 - 测试 Sora API 接口
"""
import asyncio
import aiohttp
import time
import json
from dataclasses import dataclass
from typing import Optional

# 配置
API_URL = "https://api9.de/v1/chat/completions"
API_KEY = "sk-cHsB3TpR87somlEf6mkrW3DfNNRzIRZHbNY4q00VHXQXRYql"
TIMEOUT = 1000  # 超时时间（秒）
CONCURRENT_COUNT = 1000  # 并发数量

@dataclass
class TestResult:
    success: bool
    duration: float
    error: Optional[str] = None
    response_data: Optional[str] = None

async def make_request(session: aiohttp.ClientSession, request_id: int) -> TestResult:
    """发送单个请求"""
    start_time = time.time()
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "sora-video-landscape-10s",
        "messages": [{"role": "user", "content": "一只小猫在草地上奔跑"}],
        "stream": True
    }

    try:
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        async with session.post(API_URL, headers=headers, json=payload, timeout=timeout) as response:
            duration = time.time() - start_time
            
            if response.status != 200:
                error_text = await response.text()
                return TestResult(
                    success=False,
                    duration=duration,
                    error=f"HTTP {response.status}: {error_text[:200]}"
                )
            
            # 处理 SSE 流式响应
            full_response = []
            async for line in response.content:
                decoded = line.decode('utf-8').strip()
                if decoded.startswith('data: '):
                    data = decoded[6:]
                    if data != '[DONE]':
                        full_response.append(data)
            
            return TestResult(
                success=True,
                duration=duration,
                response_data=f"收到 {len(full_response)} 条数据"
            )
            
    except asyncio.TimeoutError:
        duration = time.time() - start_time
        return TestResult(success=False, duration=duration, error="请求超时")
    except Exception as e:
        duration = time.time() - start_time
        return TestResult(success=False, duration=duration, error=str(e))

async def run_concurrent_test(concurrent_count: int):
    """运行并发测试"""
    print(f"\n{'='*60}")
    print(f"开始并发测试 - 并发数: {concurrent_count}")
    print(f"API: {API_URL}")
    print(f"超时时间: {TIMEOUT}秒")
    print(f"{'='*60}\n")
    
    connector = aiohttp.TCPConnector(limit=concurrent_count, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        start_time = time.time()
        
        # 创建并发任务
        tasks = [make_request(session, i) for i in range(concurrent_count)]
        results = await asyncio.gather(*tasks)
        
        total_time = time.time() - start_time
    
    # 统计结果
    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    durations = [r.duration for r in results]
    
    print(f"\n{'='*60}")
    print("测试结果统计")
    print(f"{'='*60}")
    print(f"总请求数: {len(results)}")
    print(f"成功数: {success_count}")
    print(f"失败数: {fail_count}")
    print(f"成功率: {success_count/len(results)*100:.2f}%")
    print(f"总耗时: {total_time:.2f}秒")
    print(f"平均响应时间: {sum(durations)/len(durations):.2f}秒")
    print(f"最快响应: {min(durations):.2f}秒")
    print(f"最慢响应: {max(durations):.2f}秒")

    # 打印每个请求的详细结果
    print(f"\n{'='*60}")
    print("详细结果")
    print(f"{'='*60}")
    for i, result in enumerate(results):
        status = "✓ 成功" if result.success else "✗ 失败"
        print(f"请求 {i+1}: {status} | 耗时: {result.duration:.2f}秒", end="")
        if result.error:
            print(f" | 错误: {result.error}")
        elif result.response_data:
            print(f" | {result.response_data}")
        else:
            print()
    
    # 返回统计数据
    return {
        "total": len(results),
        "success": success_count,
        "fail": fail_count,
        "success_rate": success_count/len(results)*100,
        "total_time": total_time,
        "avg_duration": sum(durations)/len(durations),
        "min_duration": min(durations),
        "max_duration": max(durations),
        "errors": [r.error for r in results if r.error]
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="API 并发测试工具")
    parser.add_argument("-c", "--concurrent", type=int, default=CONCURRENT_COUNT, 
                        help=f"并发数量 (默认: {CONCURRENT_COUNT})")
    parser.add_argument("-t", "--timeout", type=int, default=TIMEOUT,
                        help=f"超时时间秒 (默认: {TIMEOUT})")
    args = parser.parse_args()
    
    TIMEOUT = args.timeout
    asyncio.run(run_concurrent_test(args.concurrent))
