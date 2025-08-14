#!/usr/bin/env python3
"""
全カテゴリ（地域・価格・特徴）のデータを取得してマージ
"""
from scraper.yuryoweb_scraper import YuryoWebScraper
import json
import csv
from datetime import datetime
from collections import defaultdict

def fetch_all_categories_and_merge():
    """全カテゴリを取得してマージ"""
    
    print("="*80)
    print("全カテゴリデータの取得とマージ")
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # 既存の業種データを読み込み
    print("\n既存の業種データを読み込み中...")
    industry_data = []
    try:
        with open('industry_all_complete_20250813_233813.json', 'r', encoding='utf-8') as f:
            industry_data = json.load(f)
        print(f"  業種データ: {len(industry_data):,}件")
    except Exception as e:
        print(f"  エラー: {e}")
    
    # スクレイパー初期化
    print("\nスクレイパーを初期化中...")
    scraper = YuryoWebScraper(delay_seconds=1.5)
    
    # カテゴリ情報取得
    print("カテゴリ情報を取得中...")
    categories = scraper.fetch_categories()
    
    all_data = industry_data.copy()
    new_data = []
    
    # 地域、価格、特徴カテゴリのデータを取得（サンプル）
    for group_name in ['area', 'price', 'feature']:
        category_list = categories.get(group_name, [])
        
        print(f"\n{group_name.upper()}グループ: {len(category_list)}カテゴリ")
        
        # 各カテゴリから最初の20件だけ取得（テスト用）
        for idx, category in enumerate(category_list[:3], 1):  # 最初の3カテゴリのみ
            print(f"  [{idx}] {category['name']}: ", end="", flush=True)
            
            count = 0
            try:
                for data in scraper.scrape_category(
                    category_url=category['url'],
                    limit=20,  # 20件のみ
                    category_group=group_name,
                    category_name=category['name']
                ):
                    new_data.append(data)
                    all_data.append(data)
                    count += 1
                
                print(f"{count}件")
                
            except Exception as e:
                print(f"エラー: {e}")
    
    print(f"\n新規取得: {len(new_data)}件")
    print(f"全データ: {len(all_data)}件")
    
    # ユニーク企業を集計
    companies = defaultdict(lambda: {
        'company_name': '',
        'official_site_url': '',
        'yuryoweb_url': '',
        'addresses': set(),
        'area_categories': set(),
        'price_categories': set(),
        'feature_categories': set(),
        'industry_categories': set()
    })
    
    for record in all_data:
        url = record.get('official_site_url', '').strip()
        if not url:
            continue
        
        company = companies[url]
        company['company_name'] = record.get('company_name', '')
        company['official_site_url'] = url
        company['yuryoweb_url'] = record.get('yuryoweb_url', '')
        
        address = record.get('address', '').strip()
        if address:
            company['addresses'].add(address)
        
        category_group = record.get('category_group', '')
        category_name = record.get('category_name', '')
        
        if category_group == 'area':
            company['area_categories'].add(category_name)
        elif category_group == 'price':
            company['price_categories'].add(category_name)
        elif category_group == 'feature':
            company['feature_categories'].add(category_name)
        elif category_group == 'industry':
            company['industry_categories'].add(category_name)
    
    print(f"\nユニーク企業数: {len(companies):,}社")
    print(f"目標（11,341社）との差: {11341 - len(companies):+,}社")
    
    # テスト用JSONを保存
    if new_data:
        test_file = f"test_all_categories_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=2)
        print(f"\nテストデータ保存: {test_file}")
    
    return len(companies)

if __name__ == "__main__":
    fetch_all_categories_and_merge()