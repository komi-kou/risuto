#!/usr/bin/env python3
"""
全てのJSONファイルから全ユニーク企業を抽出
"""
import json
import csv
import os
from datetime import datetime
from collections import defaultdict

def find_all_companies():
    """全JSONファイルから全ユニーク企業を抽出"""
    
    print("="*80)
    print("全データファイルからユニーク企業を検索")
    print(f"検索時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # 全てのJSONファイルを検索
    all_unique_companies = {}
    category_stats = defaultdict(lambda: {'files': [], 'records': 0, 'unique': set()})
    
    json_files = []
    for file in os.listdir('.'):
        if file.endswith('.json'):
            json_files.append(file)
    
    print(f"\n見つかったJSONファイル: {len(json_files)}個")
    
    for file in sorted(json_files):
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                if isinstance(data, list) and len(data) > 0:
                    # カテゴリグループを判定
                    sample = data[0] if isinstance(data[0], dict) else {}
                    group = sample.get('category_group', 'unknown')
                    
                    print(f"\n{file}:")
                    print(f"  レコード数: {len(data)}")
                    print(f"  カテゴリグループ: {group}")
                    
                    # ユニーク企業を抽出
                    file_unique = set()
                    for record in data:
                        if isinstance(record, dict):
                            url = record.get('official_site_url', '').strip()
                            if url:
                                file_unique.add(url)
                                if url not in all_unique_companies:
                                    all_unique_companies[url] = record
                                
                                # カテゴリ別の統計
                                cat_group = record.get('category_group', 'unknown')
                                category_stats[cat_group]['records'] += 1
                                category_stats[cat_group]['unique'].add(url)
                    
                    print(f"  ユニーク企業: {len(file_unique)}社")
                    
                    # ファイル別統計を保存
                    if group != 'unknown':
                        category_stats[group]['files'].append(file)
                        
        except Exception as e:
            print(f"\n{file}: エラー - {e}")
    
    print(f"\n{'='*80}")
    print(f"統計サマリー:")
    print(f"{'='*80}")
    
    # カテゴリグループ別の統計
    for group, stats in sorted(category_stats.items()):
        print(f"\n{group}グループ:")
        print(f"  ファイル数: {len(set(stats['files']))}個")
        print(f"  総レコード数: {stats['records']:,}件")
        print(f"  ユニーク企業数: {len(stats['unique']):,}社")
    
    print(f"\n{'='*80}")
    print(f"全体統計:")
    print(f"  総ユニーク企業数: {len(all_unique_companies):,}社")
    print(f"  目標数(11,341社)との差: {11341 - len(all_unique_companies):+,}社")
    print(f"{'='*80}")
    
    # CSVファイルに保存
    if all_unique_companies:
        output_file = f"all_unique_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # ヘッダー
            writer.writerow([
                'company_name',
                'official_site_url',
                'address',
                'yuryoweb_url',
                'category_group',
                'category_name'
            ])
            
            # データ
            for company in all_unique_companies.values():
                writer.writerow([
                    company.get('company_name', ''),
                    company.get('official_site_url', ''),
                    company.get('address', ''),
                    company.get('yuryoweb_url', ''),
                    company.get('category_group', ''),
                    company.get('category_name', '')
                ])
        
        print(f"\n✅ CSVファイル作成完了")
        print(f"   ファイル名: {output_file}")
        print(f"   企業数: {len(all_unique_companies):,}社")
    
    return len(all_unique_companies)

if __name__ == "__main__":
    find_all_companies()