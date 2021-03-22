"""cvat のタスクを調べ、アノテーションを所定の場所に保存する。
保存されるファイル名は <タスク名>_<日付>.xml となる。同名のファイルが存在する
場合は <タスク名>_<日付>_<番号>.xml となる。この場合番号は 00 からファイル名が
かぶらない番号が付けられる。

このプログラムは前回アノテーションを取得された時のデータの更新日時が覚えられ、
その日時から更新が入っていないタスクについてはなにもしない。

前回アノテーションを更新日時は、 cvat_task_timestamp.csv というファイルに記載される。

必須モジュール
requests
"""
import os
import sys
import pprint
import requests
import json
from io import BytesIO
from zipfile import ZipFile
import time
import urllib3

# ルートURL の後に /api/v1 を必ずつける
CVAT_API_URL = "https://cvat.org/api/v1"
CVAT_LOGIN_USER = "<your login user>"
CVAT_LOGIN_EMAIL = "<your email address>"
CVAT_LOGIN_PASSWORD = "<your password>"

SAVE_PATH = r"<backup-path eg. c:\temp>"

def saving_name(task_name):
    time_suffix = time.strftime("%Y%m%d")
    full_name = os.path.join(SAVE_PATH, "%s_%s.xml" % (task_name, time_suffix))
    n = 0
    while os.path.exists(full_name):
        full_name = os.path.join(SAVE_PATH, "%s_%s_%02d.xml" % (task_name, time_suffix, n))
        n += 1
    return full_name

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {
    "accept": "application/json",
    "Content-Type": "application/json",
}

MAX_RETRY = 3
TIMESTAMP_FILE = "cvat_task_timestamp.csv"

class TaskTimestamp:
    """タスク毎に更新日時を覚えておくクラス"""
    def __init__(self):
        self._tspfile = os.path.join(os.path.dirname(__file__), TIMESTAMP_FILE)
        self._task_tsp = {}

    def read(self):
        """記録されたタイムスタンプファイルを読み込む"""
        if os.path.exists(self._tspfile):
            with open(self._tspfile) as f:
                for line in f:
                    v = line.rstrip().split("\t")
                    self._task_tsp[int(v[0])] = (v[1], v[2])
    
    def write(self):
        """タイムスタンプファイルへの書き込み"""
        with open(self._tspfile, "w") as f:
            for key, value in self._task_tsp.items():
                print("%s\t%s\t%s" % (key, value[0], value[1]), file=f)

    def is_newer(self, task_id, upd_time):
        """与えられた task_id のものの更新日時が前回記録したものより新しいか
        記録そのものがない時に真を返す"""
        if task_id in self._task_tsp:
            return upd_time > self._task_tsp[task_id][1]
        else:
            return True
    
    def set_update_time(self, task_id, name, upd_time):
        self._task_tsp[task_id] = (name, upd_time)

def main():
    #GETパラメータはparams引数に辞書で指定する
    payload = {
        'username': CVAT_LOGIN_USER,
        'email': CVAT_LOGIN_EMAIL,
        'password': CVAT_LOGIN_PASSWORD
    }
    json_data = json.dumps(payload).encode("utf-8")
    response = requests.post(
        f'{CVAT_API_URL}/auth/login',
        headers=headers,
        data=json_data,
        verify=False
    )
    if response.status_code != 200:
        raise Exception("cvat にログインできません Status=%d" % (response.status_code,))
    login_response = response.json()
    try:
        key = login_response['key']
    except:
        raise Exception("cvat にログインできません (key が見つからない)")
    headers.update(
        {'Authorization': 'Token ' + key}
    )
    response = requests.get(
        f'{CVAT_API_URL}/tasks',
        headers = headers,
        verify=False
       )
    tasks  = response.json()
    tsp = TaskTimestamp()
    tsp.read()
    for task in tasks['results']:
        task_id = task['id']
        task_name = task['name']
        upd_time = task['updated_date']
        if not tsp.is_newer(task_id, upd_time):
            print("%s[#%s] 変更なし" % (task_name, task_id))
            continue
        again = True
        retry_cnt = 0
        while again and retry_cnt < MAX_RETRY:
            retry_cnt += 1
            response = requests.get(
                f'{CVAT_API_URL}/tasks/{task_id}/annotations',
                 params = {
                     "format": "CVAT for images 1.1",
                     #"format": "COCO 1.0",
                     "filename": "output.zip",
                     "action": "download"
                 },
                 headers = headers,
                 verify = False
            )
            if response.status_code == 200:
                zip_contents = ZipFile(BytesIO(response.content))
                for name in zip_contents.namelist():
                    b = zip_contents.read(name)
                    s = b.decode("utf-8")
                    sav_name = saving_name(task_name)
                    with open(sav_name, "wb") as outf:
                        outf.write(b)
                    print("%s[#%s] 保存 %s" % (task_name, task_id, sav_name))
                tsp.set_update_time(task_id, task_name, upd_time)
                again = False
            else:
                time.sleep(1)
        if retry_cnt == MAX_RETRY:
            raise Exception("cvat からタスクデータの読み込み中にエラーが発生しました %s" % (response.status_code,))
    tsp.write()

if __name__== '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        sys.exit(1)
    sys.exit(0)
    