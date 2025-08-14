#!/usr/bin/env python3
"""
地域・価格・特徴カテゴリのデータを取得
"""
from scraper.yuryoweb_scraper import YuryoWebScraper
import json
import time
from datetime import datetime

def fetch_area_price_feature():
    """地域・価格・特徴カテゴリを取得"""
    
    print("="*80)
    print("地域・価格・特徴カテゴリの取得")
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # スクレイパー初期化
    scraper = YuryoWebScraper(delay_seconds=1.5)
    
    # カテゴリ情報取得
    print("\nカテゴリ情報を取得中...")
    categories = scraper.fetch_categories()
    
    all_data = []
    
    # 地域、価格、特徴カテゴリのデータを取得
    for group_name in ['area', 'price', 'feature']:
        category_list = categories.get(group_name, [])
        
        print(f"\n{'='*60}")
        print(f"{group_name.upper()}グループ: {len(category_list)}カテゴリ")
        print(f"{'='*60}")
        
        group_data = []
        
        for idx, category in enumerate(category_list, 1):
            print(f"\n[{idx}/{len(category_list)}] {category['name']}")
            print(f"  URL: {category['url']}")
            
            count = 0
            category_data = []
            
            try:
                start_time = time.time()
                
                for data in scraper.scrape_category(
                    category_url=category['url'],
                    limit=None,  # 全件取得
                    category_group=group_name,
                    category_name=category['name']
                ):
                    category_data.append(data)
                    all_data.append(data)
                    group_data.append(data)
                    count += 1
                    
                    if count % 100 == 0:
                        print(f"    {count}件取得済み...", end='\r')
                
                elapsed = time.time() - start_time
                print(f"  ✅ {count}件取得完了 ({elapsed:.1f}秒)")
                
                # 5カテゴリごとに中間保存
                if idx % 5 == 0:
                    temp_file = f"{group_name}_temp_{idx}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(temp_file, 'w', encoding='utf-8') as f:
                        json.dump(group_data, f, ensure_ascii=False, indent=2)
                    print(f"  💾 中間保存: {temp_file} ({len(group_data)}件)")
                
            except Exception as e:
                print(f"  ❌ エラー: {e}")
                continue
        
        # グループ全体を保存
        group_file = f"{group_name}_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(group_file, 'w', encoding='utf-8') as f:
            json.dump(group_data, f, ensure_ascii=False, indent=2)
        print(f"\n✅ {group_name.upper()}グループ保存: {group_file}")
        print(f"   取得件数: {len(group_data)}件")
    
    # 全データを保存
    output_file = f"area_price_feature_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*80}")
    print(f"✅ 全データ保存完了")
    print(f"   ファイル名: {output_file}")
    print(f"   総件数: {len(all_data)}件")
    print(f"完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    return len(all_data)

if __name__ == "__main__":
    count = fetch_area_price_feature()
    print(f"\n最終取得件数: {count:,}件")