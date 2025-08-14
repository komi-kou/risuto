#!/usr/bin/env python3
"""
全データから重複を排除し、カテゴリ情報を集約したCSVを作成
"""
import json
import csv
import os
from datetime import datetime
from collections import defaultdict

def create_final_csv():
    """最終的なユニーク企業CSVを作成"""
    
    print("="*80)
    print("最終データ処理 - ユニーク企業リストの作成")
    print(f"処理開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # 全業種データを読み込み
    all_data_file = 'industry_all_complete_20250813_233813.json'
    
    if not os.path.exists(all_data_file):
        print(f"エラー: {all_data_file}が見つかりません")
        # 代替ファイルを探す
        for file in sorted(os.listdir('.'), reverse=True):
            if file.startswith('industry_') and file.endswith('.json'):
                print(f"代替ファイル使用: {file}")
                all_data_file = file
                break
    
    print(f"\nデータファイル読み込み: {all_data_file}")
    
    try:
        with open(all_data_file, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
        print(f"  総レコード数: {len(all_data):,}件")
    except Exception as e:
        print(f"エラー: {e}")
        return 0
    
    # 企業ごとにカテゴリ情報を集約
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
    
    # カテゴリ別の統計
    category_stats = defaultdict(int)
    
    print("\nデータ処理中...")
    for record in all_data:
        # ユニークキーとして公式サイトURLを使用
        url = record.get('official_site_url', '').strip()
        if not url:
            continue
        
        company = companies[url]
        company['company_name'] = record.get('company_name', '')
        company['official_site_url'] = url
        company['yuryoweb_url'] = record.get('yuryoweb_url', '')
        
        # 住所を追加
        address = record.get('address', '').strip()
        if address:
            company['addresses'].add(address)
        
        # カテゴリグループごとに分類
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
        
        category_stats[category_group] += 1
    
    print(f"\nユニーク企業数: {len(companies):,}社")
    
    # カテゴリ統計を表示
    print("\nカテゴリ別レコード数:")
    for group, count in sorted(category_stats.items()):
        print(f"  {group}: {count:,}件")
    
    # 複数カテゴリに属する企業の統計
    multi_industry = sum(1 for c in companies.values() if len(c['industry_categories']) > 1)
    multi_area = sum(1 for c in companies.values() if len(c['area_categories']) > 1)
    
    print(f"\n複数カテゴリに属する企業:")
    print(f"  複数業種: {multi_industry:,}社")
    print(f"  複数地域: {multi_area:,}社")
    
    # CSVファイルに出力
    output_file = f"yuryoweb_all_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        # ヘッダー
        writer.writerow([
            'company_name',
            'official_site_url',
            'yuryoweb_url',
            'address',
            'area_categories',
            'price_categories',
            'feature_categories',
            'industry_categories'
        ])
        
        # データ書き込み
        for company in sorted(companies.values(), key=lambda x: x['company_name']):
            writer.writerow([
                company['company_name'],
                company['official_site_url'],
                company['yuryoweb_url'],
                '｜'.join(sorted(company['addresses'])),
                '｜'.join(sorted(company['area_categories'])),
                '｜'.join(sorted(company['price_categories'])),
                '｜'.join(sorted(company['feature_categories'])),
                '｜'.join(sorted(company['industry_categories']))
            ])
    
    print(f"\n✅ CSVファイル作成完了")
    print(f"   ファイル名: {output_file}")
    print(f"   企業数: {len(companies):,}社")
    
    # 簡易版も作成（基本情報のみ）
    simple_output = f"yuryoweb_companies_simple_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(simple_output, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        # ヘッダー
        writer.writerow([
            'company_name',
            'official_site_url',
            'address',
            'main_industry'
        ])
        
        # データ書き込み
        for company in sorted(companies.values(), key=lambda x: x['company_name']):
            # メイン業種を1つ選択
            main_industry = ''
            if company['industry_categories']:
                main_industry = sorted(company['industry_categories'])[0]
            
            writer.writerow([
                company['company_name'],
                company['official_site_url'],
                '｜'.join(sorted(company['addresses'])),
                main_industry
            ])
    
    print(f"\n✅ 簡易版CSVファイル作成完了")
    print(f"   ファイル名: {simple_output}")
    
    print(f"\n処理完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    return len(companies)

if __name__ == "__main__":
    count = create_final_csv()
    print(f"\n最終企業数: {count:,}社")