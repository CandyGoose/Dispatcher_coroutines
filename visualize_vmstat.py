import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

def parse_vmstat(log_file):
    with open(log_file, 'r') as f:
        lines = f.readlines()

    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith('procs'):
            header_idx = i + 1
            break

    if header_idx is None:
        raise ValueError("Не удалось найти заголовок в файле vmstat.log")

    headers = lines[header_idx].strip().split()

    if len(headers) >= 2 and headers[0] == 'r' and headers[1] == 'b':
        headers[0] = 'procs_r'
        headers[1] = 'procs_b'

    data = []
    for line in lines[header_idx+1:]:
        if line.strip() == '':
            continue
        parts = line.strip().split()
        if len(parts) != len(headers):
            continue
        data.append(parts)

    df = pd.DataFrame(data, columns=headers)

    numeric_cols = headers.copy()

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['time'] = pd.to_datetime(df.index, unit='s')

    return df

def plot_vmstat(df, output_dir):
    plt.figure(figsize=(12, 8))
    plt.plot(df['time'], df['us'], label='User')
    plt.plot(df['time'], df['sy'], label='System')
    plt.plot(df['time'], df['id'], label='Idle')
    plt.xlabel('Время')
    plt.ylabel('Процент')
    plt.title('CPU Usage Over Time')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(output_dir, "cpu_usage.png"))
    plt.close()

    plt.figure(figsize=(12, 8))
    plt.plot(df['time'], df['si'], label='Swap In')
    plt.plot(df['time'], df['so'], label='Swap Out')
    plt.xlabel('Время')
    plt.ylabel('KB')
    plt.title('Swap Activity Over Time')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(output_dir, "swap_activity.png"))
    plt.close()

    print(f"Графики сохранены в директорию {output_dir}")

def main():
    if len(sys.argv) != 2:
        print("Использование: python visualize_vmstat.py <vmstat_log_file>")
        sys.exit(1)

    log_file = sys.argv[1]
    output_dir = "visualizations"

    os.makedirs(output_dir, exist_ok=True)

    try:
        df = parse_vmstat(log_file)
    except ValueError as e:
        print(f"Ошибка при парсинге vmstat логов: {e}")
        sys.exit(1)

    plot_vmstat(df, output_dir)

if __name__ == "__main__":
    main()
