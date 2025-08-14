#!/usr/bin/env python3
"""
重複分析 - なぜ11,341社から10,021社になったか調査
"""
import json
from collections import defaultdict, Counter

def analyze_duplicates():
    """重複状況を分析"""
    
    print("="*80)
    print("重複分析レポート")
    print("="*80)
    
    # 全データを読み込み
    all_records = []
    
    # 各カテゴリのデータを読み込み
    files = [
        ('area_all_data_20250812_154808.json', 'area'),
        ('price_complete_20250812_192022.json', 'price'),
        ('feature_all_data_20250812_114221.json', 'feature'),
        ('industry_all_complete_20250813_233813.json', 'industry')
    ]
    
    for filename, category in files:
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for record in data:
                    all_records.append({
                        'company_name': record.get('company_name', ''),
                        'official_site_url': record.get('official_site_url', ''),
                        'category_group': record.get('category_group', ''),
                        'category_name': record.get('category_name', '')
                    })
                print(f"{category:10} : {len(data):,}件")
        except Exception as e:
            print(f"{category:10} : エラー {e}")
    
    print(f"\n総レコード数: {len(all_records):,}件")
    
    # URLごとの出現回数をカウント
    url_counts = Counter()
    url_categories = defaultdict(set)
    url_names = defaultdict(set)
    
    for record in all_records:
        url = record['official_site_url'].strip()
        if url:
            url_counts[url] += 1
            url_categories[url].add(f"{record['category_group']}:{record['category_name']}")
            url_names[url].add(record['company_name'])
    
    print(f"\nユニークURL数: {len(url_counts):,}社")
    
    # 重複統計
    duplicate_stats = Counter()
    for url, count in url_counts.items():
        if count == 1:
            duplicate_stats['1回のみ'] += 1
        elif count == 2:
            duplicate_stats['2回'] += 1
        elif count <= 5:
            duplicate_stats['3-5回'] += 1
        elif count <= 10:
            duplicate_stats['6-10回'] += 1
        else:
            duplicate_stats['11回以上'] += 1
    
    print("\n重複回数の分布:")
    for label, count in sorted(duplicate_stats.items()):
        print(f"  {label:10} : {count:,}社")
    
    # 最も重複が多い企業TOP10
    print("\n最も多くのカテゴリに登録されている企業TOP10:")
    for i, (url, count) in enumerate(url_counts.most_common(10), 1):
        names = list(url_names[url])
        categories = url_categories[url]
        print(f"\n{i}. {names[0]}")
        print(f"   URL: {url}")
        print(f"   出現回数: {count}回")
        print(f"   カテゴリ数: {len(categories)}個")
        
        # カテゴリグループ別にまとめる
        groups = defaultdict(list)
        for cat in categories:
            group, name = cat.split(':', 1)
            groups[group].append(name)
        
        for group, names in groups.items():
            print(f"   {group}: {', '.join(sorted(names)[:5])}")
            if len(names) > 5:
                print(f"         他{len(names)-5}個")
    
    # 理論値の計算
    print("\n="*60)
    print("理論値計算:")
    print(f"  総レコード数: {len(all_records):,}件")
    print(f"  ユニーク企業数: {len(url_counts):,}社")
    print(f"  平均重複率: {len(all_records) / len(url_counts):.2f}回/社")
    
    # 優良WEBの表示件数11,341との関係
    print(f"\n優良WEB表示件数: 11,341件")
    print(f"実際のユニーク企業: {len(url_counts):,}社")
    print(f"差分: {11341 - len(url_counts):,}社")
    
    print("\n考察:")
    print("優良WEBの「11,341件」は検索結果の延べ件数で、")
    print("同じ企業が複数のカテゴリに表示されているため、")
    print(f"実際のユニーク企業数は{len(url_counts):,}社となっています。")
    
    print("="*80)

if __name__ == "__main__":
    analyze_duplicates()