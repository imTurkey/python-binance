#!/bin/bash

# 设置最大重试次数（可选）
MAX_RETRIES=100
RETRY_COUNT=0

while true; do
    # 执行 Python 脚本
    python3 save_lob_data_futures.py

    # 检查上一条命令的退出状态码
    if [ $? -eq 0 ]; then
        echo "Python script executed successfully."
        break  # 如果成功，则退出循环
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo "Python script failed. Retrying in 5 seconds... (Attempt $RETRY_COUNT)"

        if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
            echo "Reached maximum number of retries. Exiting."
            exit 1  # 达到最大重试次数后退出
        fi

        sleep 5  # 等待 5 秒
    fi
done

echo "Task completed."