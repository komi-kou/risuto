#!/usr/bin/env python3
"""
優良WEBから地域・価格・特徴データを簡易取得テスト
"""
import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime

def quick_test():
    """簡易テスト取得"""
    
    print("="*80)
    print("優良WEB データ取得テスト")
    print(f"開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    base_url = "https://yuryoweb.com"
    
    # セッション作成
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    })
    
    # メインページを取得
    print("\nメインページを取得中...")
    try:
        response = session.get(base_url)
        print(f"ステータス: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # ページタイトル確認
            title = soup.find('title')
            if title:
                print(f"ページタイトル: {title.text}")
            
            # リンクを探す
            links = soup.find_all('a', href=True)
            print(f"\n見つかったリンク数: {len(links)}")
            
            # カテゴリっぽいリンクを探す
            category_links = []
            for link in links:
                href = link.get('href', '')
                text = link.text.strip()
                
                if any(keyword in text for keyword in ['地域', '価格', '特徴', '業種', '東京', '大阪', '万円']):
                    category_links.append({
                        'text': text,
                        'url': href if href.startswith('http') else base_url + href
                    })
            
            print(f"\nカテゴリ候補リンク:")
            for i, link in enumerate(category_links[:20], 1):
                print(f"  {i}. {link['text']}: {link['url']}")
            
            # いくつか試しにアクセス
            if category_links:
                print("\n最初のカテゴリをテスト取得...")
                test_url = category_links[0]['url']
                time.sleep(1)
                
                test_response = session.get(test_url)
                if test_response.status_code == 200:
                    test_soup = BeautifulSoup(test_response.content, 'html.parser')
                    
                    # 企業情報っぽい要素を探す
                    possible_companies = []
                    
                    # よくあるクラス名で検索
                    for class_name in ['company', 'item', 'result', 'card', 'list-item']:
                        elements = test_soup.find_all(class_=lambda x: x and class_name in x.lower() if x else False)
                        if elements:
                            print(f"  {class_name}クラスの要素: {len(elements)}個")
                            possible_companies.extend(elements[:3])
                    
                    # 構造を分析
                    if possible_companies:
                        print("\n企業情報の構造サンプル:")
                        for i, elem in enumerate(possible_companies[:2], 1):
                            print(f"\n  サンプル{i}:")
                            print(f"    タグ: {elem.name}")
                            print(f"    クラス: {elem.get('class')}")
                            print(f"    テキスト(最初の100文字): {elem.text.strip()[:100]}")
            
    except Exception as e:
        print(f"エラー: {e}")
    
    print(f"\n完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

if __name__ == "__main__":
    quick_test()