#!/bin/bash

force_run=false
restart_hour=5
while getopts "fr:" opt; do
  case "$opt" in
    f) force_run=true ;;
    r) restart_hour=$OPTARG ;;
  esac
done

# 0:00～6:00の間は放送休止があって408になるので処理をスキップ
current_hour=$(date +'%H')
if [ "$force_run" = false ] && [ "$current_hour" -ne "$restart_hour" ] && [ "$current_hour" -ge 0 ] && [ "$current_hour" -lt 6 ]; then
  echo "放送休止のため0～6時はチェックしない（現在 $current_hour 時）"
  exit 0
fi

declare -A service_ids=(
  [MX1]="3239123608"
  [CX]="3274001056"
  [TBS]="3273901048"
  [TX]="3274201072"
  [EX]="3274101064"
  [NTV]="3273801040"
  [NHKE]="3273701032"
  [NHKG]="3273601024"
)

tuners=$(curl -s "$TUNER_URL/api/tuners")
if [ $? -ne 0 ]; then
  echo "$TUNER_URL がおかしい"
  exit 1
fi

no_free=$(echo "$tuners" | jq 'all(.isFree == false)')
if [ "$no_free" == "true" ]; then
  echo "あきなし、チェックしない"
  exit 0
fi

if [ $(("10#$current_hour")) == "$restart_hour" ]; then
  echo "再起動時間なので、可能であれば再起動します..."
  no_use=$(curl -s "$TUNER_URL/api/tuners" | jq 'all(.isUsing == false)')
  if [ "$no_use" == "true" ]; then
    exit 2
  else
    echo "使われててだめだった"
    exit 1
  fi
fi

# 最大並列数
maxP=$(echo "$tuners" | jq '[.[] | select(.isFree == true)] | length')
max_retries=3
retry_count=0

# 最大４並列
while [ $retry_count -lt $max_retries ]; do
  failure_occurred=false

  for id in "${!service_ids[@]}"; do
    echo "$id ${service_ids[$id]}"
  done | xargs -n 2 -P $maxP bash -c '

c() {
local name="$1 $2"
local service_id="$2"
wait_sec=1
stream_url="$TUNER_URL/api/services/$service_id/stream"

# エンドポイントから最初の100バイトまでのデータを取得
output_json=$(curl -s --max-time 30 "$stream_url" | dd bs=1 count=1000 2>/dev/null | tr -d "\0")

# 取得したデータがJSONかどうかをチェック
if [ -z "$output_json" ]; then
  echo "$name Error: 空っぽ"
  return 1
else
  jq -e . >/dev/null 2>&1 <<< "$output_json"
  case $? in
    0)
      echo "$name Error: JSONなのでだめ $output_json"
      return 1
      ;;
    127)
      echo "$name Error: jqじゃない"
      return 1
      ;;
    *)
      # 5秒dropチェック
      # 切り替えるとドロップるので1秒捨てる
      timeout $wait_sec curl -s -o - $stream_url 1> /dev/null 2> /dev/null
      output=$(timeout 5 curl -s -o - $stream_url | tsselect - 2> /dev/null)
      if [ $? -ne 0 ] || [ -z "$output" ]; then
        echo "$name Error: TSじゃない"
        return 1
      elif echo "$output" | grep -v "d=  0" > /dev/null; then
        echo "$name Error: dropあり"
        return 1
      else
        # 正常、dropなし
        return 0
      fi
      ;;
  esac
fi
echo "ここにはこない"
return 1
}

c $0 $1 1

  ' || failure_occurred=true

  if $failure_occurred; then
    ((retry_count++))
    echo "失敗あり。リトライ中... ($retry_count/$max_retries)"
    sleep 2
  else
    echo "全てのサービスが正常に処理されました。"
    exit 0
  fi
done

echo "全てのリトライが失敗しました。可能であれば再起動します..."
no_use=$(curl -s "$TUNER_URL/api/tuners" | jq 'all(.isUsing == false)')
if [ "$no_use" == "true" ]; then
  exit 2
else
  echo "使われててだめだった"
  exit 1
fi
