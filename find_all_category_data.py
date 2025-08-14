#!/usr/bin/env python3
"""
全カテゴリ（地域・価格・特徴・業種）のデータを探して統合
"""
import json
import csv
import os
from datetime import datetime
from collections import defaultdict
import glob

def find_and_merge_all_data():
    """全カテゴリのデータを探して統合"""
    
    print("="*80)
    print("全カテゴリデータの検索と統合")
    print(f"処理開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # すべてのデータを格納
    all_data = []
    category_counts = defaultdict(int)
    file_info = []
    
    # 1. JSONファイルから読み込み
    print("\n1. JSONファイルを検索中...")
    json_files = glob.glob('*.json')
    
    for file in json_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    # カテゴリグループを判定
                    groups = defaultdict(int)
                    for record in data:
                        if isinstance(record, dict):
                            group = record.get('category_group', 'unknown')
                            groups[group] += 1
                    
                    # 主要なグループを特定
                    main_group = max(groups.items(), key=lambda x: x[1])[0] if groups else 'unknown'
                    
                    if main_group != 'unknown':
                        all_data.extend(data)
                        file_info.append(f"  {file}: {main_group}グループ, {len(data)}件")
                        for group, count in groups.items():
                            category_counts[group] += count
                        
        except Exception as e:
            pass
    
    if file_info:
        print("見つかったJSONファイル:")
        for info in file_info[:10]:  # 最初の10個を表示
            print(info)
    
    # 2. 既存のCSVファイルから読み込み（地域データなどが含まれている可能性）
    print("\n2. CSVファイルを検索中...")
    csv_files = glob.glob('*.csv')
    
    # 過去の会話で言及されたファイル名
    target_csv_files = [
        'all_companies_merged_20250812_203443.csv',
        'all_companies_merged_20250812_200904.csv',
        'all_companies_merged_20250812_200849.csv',
        'all_companies_merged_20250812_161609.csv'
    ]
    
    # 上位ディレクトリも検索
    for parent_path in ['../', '../../', '../../../']:
        for pattern in ['all_companies_merged*.csv', '*.csv']:
            found_files = glob.glob(parent_path + pattern)
            csv_files.extend(found_files)
    
    csv_data_found = False
    for file in csv_files:
        if 'yuryoweb' in file or 'companies_unique' in file or 'all_unique' in file:
            continue  # 今回作成したファイルはスキップ
            
        try:
            with open(file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                records = list(reader)
                
                if records and 'category_group' in records[0]:
                    # カテゴリグループ別に集計
                    groups = defaultdict(int)
                    for record in records:
                        group = record.get('category_group', '')
                        if group:
                            groups[group] += 1
                            
                            # JSONと同じ形式に変換
                            json_record = {
                                'company_name': record.get('company_name', ''),
                                'official_site_url': record.get('official_site_url', ''),
                                'address': record.get('address', ''),
                                'yuryoweb_url': record.get('yuryoweb_url', ''),
                                'category_group': group,
                                'category_name': record.get('category_name', '')
                            }
                            all_data.append(json_record)
                    
                    if groups:
                        csv_data_found = True
                        print(f"\n  {file}:")
                        for group, count in groups.items():
                            print(f"    {group}: {count}件")
                            category_counts[group] += count
                            
        except Exception as e:
            pass
    
    if not csv_data_found:
        print("  カテゴリ情報を含むCSVファイルは見つかりませんでした")
    
    # 3. データの統合と重複排除
    print(f"\n3. データ統合中...")
    print(f"  総レコード数: {len(all_data):,}件")
    
    print("\nカテゴリグループ別レコード数:")
    for group, count in sorted(category_counts.items()):
        print(f"  {group}: {count:,}件")
    
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
    
    print(f"\n✅ ユニーク企業数: {len(companies):,}社")
    
    # 目標との比較
    target = 11341
    diff = len(companies) - target
    print(f"\n目標企業数: {target:,}社")
    print(f"差分: {diff:+,}社")
    
    if len(companies) > 0:
        # 完全版CSVを作成
        output_file = f"yuryoweb_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
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
        
        print(f"\n✅ 完全版CSVファイル作成")
        print(f"   ファイル名: {output_file}")
        print(f"   企業数: {len(companies):,}社")
    
    print(f"\n処理完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    return len(companies)

if __name__ == "__main__":
    count = find_and_merge_all_data()
    print(f"\n最終結果: {count:,}社")