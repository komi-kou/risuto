#!/usr/bin/env python3
"""
全カテゴリ（地域・価格・特徴・業種）のデータをマージして最終CSV作成
"""
import json
import csv
from datetime import datetime
from collections import defaultdict

def merge_all_categories():
    """全カテゴリをマージ"""
    
    print("="*80)
    print("全カテゴリデータのマージ")
    print(f"処理開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    all_data = []
    
    # 1. 地域データを読み込み
    print("\n1. 地域データを読み込み中...")
    try:
        with open('area_all_data_20250812_154808.json', 'r', encoding='utf-8') as f:
            area_data = json.load(f)
            all_data.extend(area_data)
            print(f"   ✅ {len(area_data):,}件")
    except Exception as e:
        print(f"   ❌ エラー: {e}")
    
    # 2. 価格データを読み込み
    print("\n2. 価格データを読み込み中...")
    try:
        with open('price_complete_20250812_192022.json', 'r', encoding='utf-8') as f:
            price_data = json.load(f)
            all_data.extend(price_data)
            print(f"   ✅ {len(price_data):,}件")
    except Exception as e:
        print(f"   ❌ エラー: {e}")
    
    # 3. 特徴データを読み込み
    print("\n3. 特徴データを読み込み中...")
    try:
        with open('feature_all_data_20250812_114221.json', 'r', encoding='utf-8') as f:
            feature_data = json.load(f)
            all_data.extend(feature_data)
            print(f"   ✅ {len(feature_data):,}件")
    except Exception as e:
        print(f"   ❌ エラー: {e}")
    
    # 4. 業種データを読み込み
    print("\n4. 業種データを読み込み中...")
    try:
        with open('industry_all_complete_20250813_233813.json', 'r', encoding='utf-8') as f:
            industry_data = json.load(f)
            all_data.extend(industry_data)
            print(f"   ✅ {len(industry_data):,}件")
    except Exception as e:
        print(f"   ❌ エラー: {e}")
    
    print(f"\n総レコード数: {len(all_data):,}件")
    
    # カテゴリグループ別集計
    category_counts = defaultdict(int)
    for record in all_data:
        group = record.get('category_group', 'unknown')
        category_counts[group] += 1
    
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
    if diff >= 0:
        print("🎉 目標達成！")
    
    # カテゴリ充実度の分析
    print("\nカテゴリ充実度分析:")
    stats = {
        'area_only': 0,
        'price_only': 0,
        'feature_only': 0,
        'industry_only': 0,
        'multiple': 0,
        'all_categories': 0
    }
    
    for company in companies.values():
        has_area = len(company['area_categories']) > 0
        has_price = len(company['price_categories']) > 0
        has_feature = len(company['feature_categories']) > 0
        has_industry = len(company['industry_categories']) > 0
        
        category_count = sum([has_area, has_price, has_feature, has_industry])
        
        if category_count == 1:
            if has_area:
                stats['area_only'] += 1
            elif has_price:
                stats['price_only'] += 1
            elif has_feature:
                stats['feature_only'] += 1
            elif has_industry:
                stats['industry_only'] += 1
        elif category_count > 1:
            stats['multiple'] += 1
            if category_count == 4:
                stats['all_categories'] += 1
    
    print(f"  地域のみ: {stats['area_only']:,}社")
    print(f"  価格のみ: {stats['price_only']:,}社")
    print(f"  特徴のみ: {stats['feature_only']:,}社")
    print(f"  業種のみ: {stats['industry_only']:,}社")
    print(f"  複数カテゴリ: {stats['multiple']:,}社")
    print(f"  全カテゴリあり: {stats['all_categories']:,}社")
    
    # 最終CSVを作成
    output_file = f"yuryoweb_final_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
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
    
    print(f"\n✅ 最終CSVファイル作成完了")
    print(f"   ファイル名: {output_file}")
    print(f"   企業数: {len(companies):,}社")
    
    # サマリーCSVも作成（シンプル版）
    summary_file = f"yuryoweb_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(summary_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        writer.writerow([
            'company_name',
            'official_site_url',
            'address',
            'categories_count'
        ])
        
        for company in sorted(companies.values(), key=lambda x: x['company_name']):
            categories_count = (
                len(company['area_categories']) +
                len(company['price_categories']) +
                len(company['feature_categories']) +
                len(company['industry_categories'])
            )
            
            writer.writerow([
                company['company_name'],
                company['official_site_url'],
                list(company['addresses'])[0] if company['addresses'] else '',
                categories_count
            ])
    
    print(f"\n✅ サマリーCSVファイル作成完了")
    print(f"   ファイル名: {summary_file}")
    
    print(f"\n処理完了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    return len(companies)

if __name__ == "__main__":
    count = merge_all_categories()
    print(f"\n🏁 最終結果: {count:,}社")