#!/usr/bin/env python
# coding: utf-8

import csv
import re
import json
import time
import requests
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# DeepSeek 配置
# =========================
DEEPSEEK_API_URL = "***"
DEEPSEEK_API_KEY = "***"
DEEPSEEK_MODEL_NAME = "DeepSeek-R1"

# =========================
# Gemini 配置
# =========================
GEMINI_GATEWAY_URL = "***"
GEMINI_API_KEY = "***"
GEMINI_MODEL = "gemini-3-pro-preview"

# =========================
# 文件配置
# =========================
INPUT_CSV = "inner_and_overseas.csv"
OUTPUT_CSV = "cross_check_multi.csv"

BATCH_SIZE = 5
SLEEP_SECONDS = 2

# =========================
# Prompt 模板
# =========================
PROMPT_TEMPLATE = """
你是一个设备型号识别专家，擅长将原始、非标准的设备型号，映射为消费者熟知的设备品牌和型号，以及输出设备相关的参数，比如cpu型号、核数、运行内存、分辨率、屏幕刷新率。
比如：TAS-AN00品牌是Huawei、型号是Mate 30、cpu型号Kirin 990、核数等于8、运行内存是8 GB、屏幕刷新率是60 Hz。

请严格遵守以下规则（非常重要）：
1. 优先识别品牌、型号、cpu型号、核数
2. 如果无法确认字段信息，请返回 null
3. 不允许猜测、推断或编造不存在的设备信息
4. 输出必须是合法 JSON，不要包含任何解释性文字
5. 如果输出中包含除 JSON 以外的任何字符，视为错误输出

字段类型与格式要求（必须遵守）：
- origin_device_model: string
- mapped_brand: string 或 null
- mapped_device_model: string 或 null
- cpu_name: string 或 null
- cpu_core: int 或 null
- ram: string 或 null
- refresh_rate: string 或 null

结果请仅返回 JSON 数组
"""

# =========================
# 工具函数
# =========================
def extract_json(text: str):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```json", "", text)
        text = re.sub(r"^```", "", text)
        text = re.sub(r"```$", "", text)
        text = text.strip()

    first_bracket = text.find("[")
    if first_bracket == -1:
        raise ValueError("未找到 JSON 数组起始符 [")

    depth = 0
    for i in range(first_bracket, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                return json.loads(text[first_bracket:i + 1])

    raise ValueError("JSON 数组不完整")

def clean_row_keys(row):
    return {k.strip().strip('"'): v for k, v in row.items()}

def is_pure_digit(s: str):
    return s.isdigit()

def is_pure_chinese(s: str):
    return bool(re.fullmatch(r"[\u4e00-\u9fff]+", s))

# 原始字符串去掉为空、字符串个数<=4、纯中文/数字
def should_skip(device: str):
    if not device:
        return True
    if len(device) <= 4:
        return True
    if is_pure_digit(device):
        return True
    if is_pure_chinese(device):
        return True
    return False

# =========================
# DeepSeek 调用
# =========================
def call_deepseek(device_models):
    prompt = PROMPT_TEMPLATE + "\n" + "\n".join(device_models)
    payload = {
        "model": DEEPSEEK_MODEL_NAME,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 1800
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
    }
    resp = requests.post(
        DEEPSEEK_API_URL,
        headers=headers,
        data=json.dumps(payload),
        timeout=120
    )
    resp.raise_for_status()
    return extract_json(resp.json()["choices"][0]["message"]["content"])

# =========================
# Gemini 调用
# =========================
def call_gemini(device_models):
    prompt = PROMPT_TEMPLATE + "\n" + "\n".join(device_models)
    payload = {
        "model": GEMINI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 1800
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GEMINI_API_KEY}"
    }
    resp = requests.post(
        GEMINI_GATEWAY_URL,
        headers=headers,
        data=json.dumps(payload),
        timeout=120
    )
    resp.raise_for_status()
    return extract_json(resp.json()["choices"][0]["message"]["content"])

# =========================
# 多线程安全包装
# =========================
def call_deepseek_safe(dev):
    try:
        return call_deepseek([dev])
    except Exception as e:
        print(f"❌ DeepSeek failed for {dev}: {e}")
        return []
    print("thread:", threading.get_ident(), dev)
    
def call_gemini_safe(dev):
    try:
        return call_gemini([dev])
    except Exception as e:
        print(f"❌ Gemini failed for {dev}: {e}")
        return []

# =========================
# 主流程
# =========================
def main():
    output_file = Path(OUTPUT_CSV)
    file_exists = output_file.exists()

    with open(INPUT_CSV, newline="", encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.DictReader(f)
        devices = []
        for row in reader:
            row = clean_row_keys(row)
            d = row.get("origin_device_model")
            if d and not should_skip(d):
                devices.append(d)

    print("Total valid devices:", len(devices))

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f_out:
        writer = None

        for i in range(0, len(devices), BATCH_SIZE):
            batch = devices[i:i + BATCH_SIZE]
            print(f"\nProcessing batch {i // BATCH_SIZE + 1}: {batch}")

            ds_results, gm_results = [], []

            # 🔹 DeepSeek 多线程
            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                futures = [executor.submit(call_deepseek_safe, dev) for dev in batch]
                for future in as_completed(futures):
                    ds_results.extend(future.result())

            # 🔹 Gemini 多线程
            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                futures = [executor.submit(call_gemini_safe, dev) for dev in batch]
                for future in as_completed(futures):
                    gm_results.extend(future.result())

            gm_map = {
                r["origin_device_model"]: r
                for r in gm_results
                if "origin_device_model" in r
            }

            for row in ds_results:
                odm = row.get("origin_device_model")
                gm = gm_map.get(odm, {})

                row["gemini_mapped_brand"] = gm.get("mapped_brand")
                row["gemini_mapped_device_model"] = gm.get("mapped_device_model")
                row["gemini_cpu_name"] = gm.get("cpu_name")
                row["gemini_cpu_core"] = gm.get("cpu_core")
                row["gemini_ram"] = gm.get("ram")
                row["gemini_refresh_rate"] = gm.get("refresh_rate")

                if writer is None:
                    writer = csv.DictWriter(f_out, fieldnames=row.keys())
                    if not file_exists:
                        writer.writeheader()
                        file_exists = True

                writer.writerow(row)
                f_out.flush()
                print("✅ wrote:", odm)

            time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
