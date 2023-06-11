import requests
import json
import pandas as pd
import os
import sys
import re
from datetime import datetime as dt
from dotenv import load_dotenv # .envファイルから環境変数をロードするためのライブラリをインポート




def main():
    '''
    メイン関数
    '''
    # 環境変数読み込み
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')
    version = os.getenv('VERSION')
    ig_user_id = os.getenv('IG_USER_ID')
    # 情報を取得したいユーザーID
    user_id = 'yuta.tamamori_j'


    today = dt.now().strftime('%Y-%m-%d')
    # ユーザーIDを使ってビジネスディスカバリー情報の取得
    account_dict = call_business_profile(version, ig_user_id, user_id, access_token)
    # API制限に引っかかった場合の処理 account_dict['error']['code'] == 4となる場合
    try:
        if account_dict['error']['code'] == 4:
            print('API制限に引っかかりました｡1時間後に再度試してみてください｡ ', '現在時刻:',dt.now().strftime('%Y-%m-%d %H:%M:%S'))
            print('プログラムを終了します｡')
            sys.exit()
    except Exception:
        pass

    # 取得した情報をjson_normalizeで一気にデータフレーム型式に変換 その後､columnsを短い名前にrename
    dfp = pd.json_normalize(account_dict)
    dfp.rename(columns={'business_discovery.username': 'username', 'business_discovery.website': 'website',
        'business_discovery.name': 'name', 'business_discovery.id': 'id',
        'business_discovery.profile_picture_url': 'profile_picture_url',
        'business_discovery.biography': 'biography', 'business_discovery.follows_count': 'follows_count',
        'business_discovery.followers_count': 'followers_count', 'business_discovery.media_count': 'media_count'}, inplace=True)


    # 重複したカラム名があるとデータポータルで読み込めない｡元の 'id' を残して､ 置換した 'business_discovery.id'の 'id' を削除
    dfp = dfp.loc[:,~dfp.columns.duplicated()]

    # プロフィール情報のデータフレームcsv作成
    my_makedirs(f'./result/{user_id}')
    dfp.to_csv(f'./result/{user_id}/{user_id}-profile-{today}.csv')

    # メディア情報の取り出し
    # media_data の中に投稿の一個一個がリストの形式で入っている
    media_data = dfp['business_discovery.media.data'][0]
    # データフレームを作るための空の辞書を作成
    data_dict = make_dict()


    # after_keyがあれば、追加でデータを取得
    after_key = after_key_get(account_dict)

    # after_keyがある場合（投稿数が25件以上の場合）
    if after_key:
        # 追加でデータを取得する
        paginate_dict = paginate(user_id, after_key, version, ig_user_id, access_token)
        # media_data が最初の情報で､ paginate_data がafter_key以降の続きの情報
        paginate_data = paginate_dict['business_discovery']['media']['data']
        df1 = make_data_df(media_data, data_dict)
        df2 = make_data_df(paginate_data, data_dict)
        # concatを使って先に作成したデータフレームと結合する アカウントのすべての情報を持つデータフレームが完成
        df = pd.concat([df1, df2])

        # インデックスを振りなおす（結合するとインデックスがバラバラになるため）
        df.reset_index(inplace=True, drop=True)
        # 結果csv保存用のディレクトリ作成
        my_makedirs(f'./result/{user_id}')
        # 投稿情報のデータフレームcsv作成
        df.to_csv(f'./result/{user_id}/{user_id}-{today}.csv')

    # after_keyがない場合 そのままデータフレームを作成
    else:
        print('after_keyがありませんでした｡')
        df = make_data_df(media_data, data_dict)
        my_makedirs(f'./result/{user_id}')
        df.to_csv(f'./result/{user_id}/{user_id}-{today}.csv')



# 結果データ保存
def my_makedirs(path):
    # ディレクトリがなければ､ディレクトリを作るという処理
    if not os.path.isdir(path):
        os.makedirs(path)




# データフレームのデータを入れるための辞書の作成
def make_dict():
    # 空の辞書を作成 pd.DataFrame(dict)すればデータフレームが簡単にできる
    data_dict = {}
    # データフレームにするカラム名をキーとして、空のリストで初期化
    data_dict['media_url'] = []
    data_dict['caption'] = []
    data_dict['hashtag'] = []
    data_dict['timestamp'] = []
    data_dict['like_count'] = []
    data_dict['comments_count'] = []
    return data_dict



# アカウントのプロフィール情報を取得
def call_business_profile(version, ig_user_id, user_id, access_token):
    # エンドポイントの設定
    business_api = f'https://graph.facebook.com/{version}/{ig_user_id}?fields=business_discovery.username({user_id}){{username, website, name, id, profile_picture_url, biography, follows_count, followers_count, media_count, media{{timestamp, like_count, comments_count, caption, media_url}}}}&access_token={access_token}'
    # GETリクエスト
    r = requests.get(business_api)
    # JSON文字列を辞書に変換
    #+ json.loads()が辞書に変換する記述
    #+ r.contentは上で取得した r に.contentをつけることでjson文字列が取得できる
    account_dict = json.loads(r.content)
    return account_dict



# ページ送りのためのafter_key取得
def after_key_get(account_dict):
    after_key = ''
    # after_keyがある場合
    try:
        after_key = account_dict['business_discovery']['media']['paging']['cursors']['after']
        return after_key
    # after_keyがない場合
    except KeyError as e:
        print('after_key', e)
        return after_key



# 続きのデータ取得
def paginate(user_id, after_key, version, ig_user_id, access_token):
    # ビジネスディスカバリーのページ送りのエンドポイントの設定
    api_pagination = f'https://graph.facebook.com/{version}/{ig_user_id}?fields=business_discovery.username({user_id}){{media.after({after_key}).limit(300){{timestamp, like_count, comments_count, caption, media_url}}}}&access_token={access_token}'
    r = requests.get(api_pagination)
    account_dict = json.loads(r.content)
    return account_dict



# データフレームの作成
def make_data_df(media_data, data_dict):
    """
    データフレームの作成
    キーがない場合もあるので、
    try-exceptでエラー処理を記述
    """
    for i in range(len(media_data)):
        try:
            # まず要素を取り出す media_url、caption、hash_tags、timestamp、like_count、comments_count
            media_url = media_data[i]['media_url']
            caption = media_data[i]['caption']
            # hashtagはcaptionの中から正規表現を使って取り出していく
            hash_tag_list = re.findall('#[^\s→#\ufeff]*', caption)
            # リストを結合して文字列にする
            hash_tags = '\n'.join(hash_tag_list)
            # 実際のtimestamp: 2023-01-28T09:00:06+0000 のいらない部分を削除
            timestamp = media_data[i]['time_stamp'].replace('+0000', '').replace('T', ' ')
            like_count = media_data[i]['like_count']
            comments_count = media_data[i]['comments_count']

            # data_dictの各リストにappendで要素を入れていく
            data_dict['media_url'].append(media_url)
            data_dict['caption'].append(caption)
            data_dict['hashtag'].append(hash_tags)
            data_dict['timestamp'].append(timestamp)
            data_dict['like_count'].append(like_count)
            data_dict['comments_count'].append(comments_count)

        # キーがない場合の場合の処理を記述
        except KeyError as e:
            print('KeyError', e, 'というKeyが存在しません｡')
            media_url = ''
            caption = media_data[i]['caption']
            hash_tags = ''
            timestamp = media_data[i]['timestamp']
            like_count = media_data[i]['like_count']
            comments_count = media_data[i]['comments_count']

            data_dict['media_url'].append(media_url)
            data_dict['caption'].append(caption)
            data_dict['hashtag'].append(hash_tags)
            data_dict['timestamp'].append(timestamp)
            data_dict['like_count'].append(like_count)
            data_dict['comments_count'].append(comments_count)
    return pd.DataFrame(data_dict)



if __name__ == "__main__":
    main()
