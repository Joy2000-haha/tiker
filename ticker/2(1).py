import pandas as pd
import numpy as np
import wrds
import warnings
import os
from datetime import datetime
import traceback

# 忽略一些pandas的警告
warnings.filterwarnings('ignore')


class StockDescriptiveStats:
    def __init__(self, ticker_csv_path):
        """
        初始化股票描述性统计分析类
        """
        self.ticker_csv_path = ticker_csv_path
        self.db = None
        self.tickers = None
        # 初始化用于存储数据的DataFrame
        self.crsp_data = None
        self.comp_data = None
        self.ccm_link = None
        self.merged_data = None
        self.final_stats = None
        self.results = None
        self.detailed_results = None
        self.summary_table = None
        self.percentile_table = None

    def connect_wrds(self):
        """连接WRDS数据库"""
        try:
            self.db = wrds.Connection()
            print("成功连接到WRDS数据库")
            return True
        except Exception as e:
            print(f"连接WRDS数据库失败: {e}")
            return False

    def load_tickers(self):
        """从CSV文件加载ticker列表"""
        try:
            ticker_df = pd.read_csv(self.ticker_csv_path)
            # 假设CSV文件有一列包含ticker符号
            possible_columns = ['ticker', 'TICKER', 'symbol', 'SYMBOL', 'permno', 'PERMNO']
            ticker_column = None
            for col in possible_columns:
                if col in ticker_df.columns:
                    ticker_column = col
                    break
            if ticker_column is None:
                ticker_column = ticker_df.columns[0]
                print(f"未找到标准ticker列名，将使用第一列: '{ticker_column}'")

            self.tickers = ticker_df[ticker_column].dropna().unique().tolist()
            print(f"成功加载 {len(self.tickers)} 个股票代码")
            return True
        except Exception as e:
            print(f"加载ticker列表失败: {e}")
            return False

    def get_ccm_link(self):
        """获取CRSP-Compustat链接表"""
        try:
            print("正在获取CRSP-Compustat链接表...")
            ccm_query = """
                SELECT gvkey, lpermno as permno, linktype, linkprim, linkdt, linkenddt
                FROM crsp.ccmxpf_linktable
                WHERE linktype IN ('LU', 'LC') AND linkprim IN ('P', 'C')
            """
            self.ccm_link = self.db.raw_sql(ccm_query, date_cols=['linkdt', 'linkenddt'])

            # 处理链接结束日期（如果为空，设为当前日期）
            self.ccm_link['linkenddt'] = self.ccm_link['linkenddt'].fillna(pd.to_datetime('2024-12-31'))

            print(f"成功获取CCM链接表: {len(self.ccm_link)} 条记录")
            return True
        except Exception as e:
            print(f"获取CCM链接表失败: {e}")
            traceback.print_exc()
            return False

    def get_stock_data(self, start_date, end_date):
        """获取CRSP和Compustat数据"""
        if not self.tickers:
            print("Ticker列表为空，无法获取数据。")
            return False

        # --- 步骤 1: 获取CCM链接表 ---
        if not self.get_ccm_link():
            return False

        # --- 步骤 2: 获取CRSP月度数据 ---
        print("正在获取CRSP数据...")
        try:
            # 分块处理以避免查询过长
            chunk_size = 500
            all_crsp_data = []

            for i in range(0, len(self.tickers), chunk_size):
                chunk = self.tickers[i:i + chunk_size]

                # 处理单个ticker的情况（避免SQL语法错误）
                if len(chunk) == 1:
                    ticker_condition = f"= '{chunk[0]}'"
                else:
                    ticker_condition = f"IN {tuple(chunk)}"

                query = f"""
                    SELECT a.permno, a.permco, a.date, b.ticker, a.ret, a.retx, a.prc, a.shrout, a.vol
                    FROM crsp.msf as a
                    LEFT JOIN crsp.msenames as b
                    ON a.permno = b.permno AND b.namedt <= a.date AND a.date <= b.nameendt
                    WHERE a.date >= '{start_date}' AND a.date <= '{end_date}'
                    AND b.ticker {ticker_condition}
                    AND a.ret IS NOT NULL
                    ORDER BY a.permno, a.date
                """

                stock_chunk = self.db.raw_sql(query, date_cols=['date'])
                if not stock_chunk.empty:
                    all_crsp_data.append(stock_chunk)

            if not all_crsp_data:
                print("未获取到任何CRSP数据。")
                return False

            self.crsp_data = pd.concat(all_crsp_data, ignore_index=True)
            self.crsp_data.drop_duplicates(subset=['permno', 'date'], keep='first', inplace=True)
            print(f"成功获取CRSP数据: {len(self.crsp_data)} 条记录")

        except Exception as e:
            print(f"获取CRSP数据失败: {e}")
            traceback.print_exc()
            return False

        # --- 步骤 3: 获取Compustat年度数据 ---
        print("正在获取Compustat数据...")
        try:
            # 修正：使用正确的表名 comp.funda
            comp_query = f"""
                SELECT gvkey, datadate, at, lt, pstk, ceq, txditc, pstkrv, seq, pstkl
                FROM comp.funda
                WHERE datadate >= '{start_date}' AND datadate <= '{end_date}'
                AND datafmt = 'STD' AND consol = 'C' AND indfmt = 'INDL'
                AND at IS NOT NULL
                ORDER BY gvkey, datadate
            """

            self.comp_data = self.db.raw_sql(comp_query, date_cols=['datadate'])
            print(f"成功获取Compustat数据: {len(self.comp_data)} 条记录")

        except Exception as e:
            print(f"获取Compustat数据失败: {e}")
            traceback.print_exc()
            return False

        print(f"成功获取所有数据")
        return True

    def calculate_indicators(self):
        """基于获取的数据计算所需指标"""
        if self.crsp_data is None or self.comp_data is None or self.ccm_link is None:
            print("基础数据缺失，无法计算指标。")
            return False

        print("正在计算指标...")
        try:
            # --- 步骤 1: 合并CRSP和CCM链接表 ---
            print("正在合并CRSP和CCM链接表...")
            crsp_ccm = self.crsp_data.merge(self.ccm_link, on='permno', how='left')

            # 过滤有效的链接关系
            crsp_ccm = crsp_ccm[
                (crsp_ccm['date'] >= crsp_ccm['linkdt']) &
                (crsp_ccm['date'] <= crsp_ccm['linkenddt']) &
                (crsp_ccm['gvkey'].notna())
                ]

            if crsp_ccm.empty:
                print("CRSP和CCM链接后没有有效数据")
                return False

            print(f"CRSP-CCM合并后: {len(crsp_ccm)} 条记录")

            # --- 步骤 2: 处理Compustat数据 ---
            print("正在处理Compustat数据...")
            comp = self.comp_data.copy()

            # 计算账面价值 (Book Equity) - 参考Ken French's library定义
            comp['txditc'] = comp['txditc'].fillna(0)
            comp['ps'] = comp['pstkrv'].fillna(comp['pstkl']).fillna(comp['pstk']).fillna(0)
            comp['se'] = comp['seq'].fillna(comp['ceq'] + comp['pstk'].fillna(0)).fillna(
                comp['at'] - comp['lt']).fillna(0)
            comp['be'] = comp['se'] + comp['txditc'] - comp['ps']

            # 只保留账面价值为正的记录
            comp = comp[comp['be'] > 0]

            # 财务数据通常延迟6个月可用
            comp['available_date'] = comp['datadate'] + pd.DateOffset(months=6)

            print(f"处理后的Compustat数据: {len(comp)} 条记录")

            # --- 步骤 3: 合并所有数据 ---
            print("正在合并CRSP和Compustat数据...")

            # 确保数据类型正确
            crsp_ccm['date'] = pd.to_datetime(crsp_ccm['date'])
            comp['available_date'] = pd.to_datetime(comp['available_date'])

            # 确保gvkey数据类型一致
            crsp_ccm['gvkey'] = crsp_ccm['gvkey'].astype(str)
            comp['gvkey'] = comp['gvkey'].astype(str)

            # 排序数据以便使用merge_asof - 重要：必须先按by键排序，再按时间键排序
            print("正在排序数据...")
            crsp_ccm = crsp_ccm.sort_values(['gvkey', 'date']).reset_index(drop=True)
            comp = comp.sort_values(['gvkey', 'available_date']).reset_index(drop=True)

            # 检查排序是否正确
            print("验证数据排序...")
            print(f"CRSP数据样本 - gvkey: {crsp_ccm['gvkey'].head()}")
            print(f"CRSP数据样本 - date: {crsp_ccm['date'].head()}")
            print(f"Comp数据样本 - gvkey: {comp['gvkey'].head()}")
            print(f"Comp数据样本 - available_date: {comp['available_date'].head()}")

            print(f"CRSP数据类型 - gvkey: {crsp_ccm['gvkey'].dtype}, date: {crsp_ccm['date'].dtype}")
            print(f"Comp数据类型 - gvkey: {comp['gvkey'].dtype}, available_date: {comp['available_date'].dtype}")
            print(f"CRSP空值 - gvkey: {crsp_ccm['gvkey'].isna().sum()}, date: {crsp_ccm['date'].isna().sum()}")
            print(
                f"Comp空值 - gvkey: {comp['gvkey'].isna().sum()}, available_date: {comp['available_date'].isna().sum()}")

            # 移除空值
            crsp_ccm = crsp_ccm.dropna(subset=['gvkey', 'date'])
            comp = comp.dropna(subset=['gvkey', 'available_date'])

            print(f"清理后 - CRSP: {len(crsp_ccm)} 条记录, Comp: {len(comp)} 条记录")

            # 执行时间序列合并...
            print("执行时间序列合并...")

            # 使用优化的向量化匹配方案
            print("使用优化的向量化时间匹配方案...")

            # 清理和预处理数据
            print("清理和预处理数据...")

            # 去除重复记录
            crsp_ccm = crsp_ccm.drop_duplicates(subset=['gvkey', 'date']).reset_index(drop=True)
            comp = comp.drop_duplicates(subset=['gvkey', 'available_date']).reset_index(drop=True)

            print(f"去重后 - CRSP: {len(crsp_ccm)} 条记录, Comp: {len(comp)} 条记录")

            # 方法1: 尝试使用pandas的merge_asof但处理排序问题
            try:
                print("尝试修复排序后的merge_asof...")

                # 更严格的数据清理
                crsp_clean = crsp_ccm.copy()
                comp_clean = comp.copy()

                # 确保没有重复和NaN
                crsp_clean = crsp_clean.dropna(subset=['gvkey', 'date'])
                comp_clean = comp_clean.dropna(subset=['gvkey', 'available_date'])

                # 转换数据类型确保一致性
                crsp_clean['gvkey'] = crsp_clean['gvkey'].astype(str).str.strip()
                comp_clean['gvkey'] = comp_clean['gvkey'].astype(str).str.strip()

                # 强制排序
                crsp_clean = crsp_clean.sort_values(['gvkey', 'date']).reset_index(drop=True)
                comp_clean = comp_clean.sort_values(['gvkey', 'available_date']).reset_index(drop=True)

                # 验证排序
                if (crsp_clean.groupby('gvkey')['date'].is_monotonic_increasing.all() and
                        comp_clean.groupby('gvkey')['available_date'].is_monotonic_increasing.all()):

                    merged = pd.merge_asof(
                        crsp_clean,
                        comp_clean[['gvkey', 'available_date', 'be']],
                        left_on='date',
                        right_on='available_date',
                        by='gvkey',
                        direction='backward',
                        tolerance=pd.Timedelta(days=365 * 5)
                    )
                    print("修复后的merge_asof成功！")
                else:
                    raise ValueError("排序验证失败")

            except Exception as e:
                print(f"merge_asof仍然失败: {e}")
                print("使用快速向量化匹配方案...")

                # 方法2: 使用更快的向量化方法
                try:
                    # 为每个gvkey创建匹配索引
                    merged_list = []

                    # 按gvkey分组处理 - 向量化方法
                    for gvkey in crsp_ccm['gvkey'].unique():
                        if pd.isna(gvkey):
                            continue

                        crsp_subset = crsp_ccm[crsp_ccm['gvkey'] == gvkey].copy()
                        comp_subset = comp[comp['gvkey'] == gvkey].copy()

                        if comp_subset.empty:
                            continue

                        # 排序
                        crsp_subset = crsp_subset.sort_values('date').reset_index(drop=True)
                        comp_subset = comp_subset.sort_values('available_date').reset_index(drop=True)

                        # 使用numpy的searchsorted进行快速匹配
                        crsp_dates = crsp_subset['date'].values
                        comp_dates = comp_subset['available_date'].values

                        # 找到每个CRSP日期对应的最新Comp日期索引
                        indices = np.searchsorted(comp_dates, crsp_dates, side='right') - 1

                        # 只保留有效匹配（indices >= 0）
                        valid_mask = indices >= 0
                        valid_crsp = crsp_subset[valid_mask].copy()
                        valid_indices = indices[valid_mask]

                        if len(valid_crsp) > 0:
                            # 添加匹配的be值
                            valid_crsp['be'] = comp_subset.iloc[valid_indices]['be'].values
                            valid_crsp['available_date'] = comp_subset.iloc[valid_indices]['available_date'].values
                            merged_list.append(valid_crsp)

                    if merged_list:
                        merged = pd.concat(merged_list, ignore_index=True)
                        print(f"快速向量化匹配成功！合并数据: {len(merged)} 条记录")
                    else:
                        raise ValueError("向量化匹配失败")

                except Exception as e2:
                    print(f"向量化匹配也失败: {e2}")
                    print("使用最简单的年度匹配作为最后备选...")

                    # 方法3: 简单年度匹配
                    crsp_ccm['year'] = crsp_ccm['date'].dt.year
                    comp['year'] = comp['datadate'].dt.year

                    # 为每个gvkey-year组合选择最新的财务数据
                    comp_yearly = comp.sort_values(['gvkey', 'year', 'datadate']).groupby(
                        ['gvkey', 'year']).last().reset_index()

                    merged = crsp_ccm.merge(
                        comp_yearly[['gvkey', 'year', 'be']],
                        on=['gvkey', 'year'],
                        how='left'
                    )

                    # 过滤掉没有财务数据的记录
                    merged = merged[merged['be'].notna()]

                    if merged.empty:
                        print("所有合并方案都失败")
                        return False
                    else:
                        print(f"年度匹配成功！合并数据: {len(merged)} 条记录")

            # 只保留有账面价值数据的记录
            merged = merged[merged['be'].notna()]

            if merged.empty:
                print("合并后没有有效数据")
                return False

            print(f"最终合并数据: {len(merged)} 条记录")

            # --- 步骤 4: 计算关键指标 ---
            print("正在计算关键指标...")

            df = merged.copy()

            # 计算市值 (Market Equity)
            df['prc_abs'] = df['prc'].abs()
            df['me'] = df['prc_abs'] * df['shrout']  # 单位: 千美元

            # 计算账面市值比 (B/M)
            df['bm'] = (df['be'] * 1000) / df['me']  # 将账面价值从百万转为千美元

            # 计算size (取对数)
            df['size'] = np.log(df['me'])

            # 计算动量指标 (mom) - 过去12个月收益率，跳过最近1个月
            df = df.sort_values(['permno', 'date'])
            df['ret_lag1'] = df.groupby('permno')['ret'].shift(1)
            df['ret_cumulative'] = df.groupby('permno')['ret'].rolling(window=12, min_periods=8).apply(
                lambda x: (1 + x).prod() - 1, raw=False
            ).reset_index(0, drop=True)
            df['mom'] = df['ret_cumulative'] - df['ret_lag1']

            # 计算每月的截面数据个数 (n)
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['n'] = df.groupby(['year', 'month'])['permno'].transform('count')

            # 选取最终需要的列并去除缺失值
            final_columns = ['permno', 'ticker', 'date', 'year', 'month', 'ret', 'size', 'bm', 'mom', 'n']
            self.final_stats = df[final_columns].dropna(subset=['ret', 'size', 'bm'])

            print(f"指标计算完成，最终数据: {len(self.final_stats)} 条记录")
            print(f"时间范围: {self.final_stats['date'].min()} 到 {self.final_stats['date'].max()}")
            print(f"股票数量: {self.final_stats['permno'].nunique()}")

            return True

        except Exception as e:
            print(f"计算指标失败: {e}")
            traceback.print_exc()
            return False

    def calculate_descriptive_stats(self):
        """计算描述性统计 - 个股cross-sectional再time series average+std"""
        if self.final_stats is None or self.final_stats.empty:
            print("没有可用于统计的指标数据。")
            return False

        print("正在计算描述性统计...")
        print("方法：个股cross-sectional再time series average+std")
        try:
            # 定义要分析的变量
            variables = ['ret', 'size', 'bm', 'mom', 'n']
            # 扩展百分位数列表 - 从1%到99%
            percentiles = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
            # 添加更多关键百分位数
            extended_percentiles = [0.01, 0.02, 0.03, 0.04, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
                                    0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.96, 0.97, 0.98, 0.99]

            results = {}
            detailed_results = {}  # 存储详细的月度统计

            for var in variables:
                print(f"正在计算 {var} 的描述性统计...")

                if var == 'n':
                    # n的特殊处理（每月相同）
                    n_monthly = self.final_stats.groupby(['year', 'month'])[var].first()
                    results[var] = {
                        'mean': n_monthly.mean(),
                        'std': n_monthly.std(),
                        'min': n_monthly.min(),
                        'max': n_monthly.max(),
                        'count': len(n_monthly),
                        **{f'p{int(p * 100)}': np.percentile(n_monthly, p * 100) for p in extended_percentiles}
                    }
                    detailed_results[var] = {
                        'monthly_values': n_monthly,
                        'time_series_stats': {
                            'mean': n_monthly.mean(),
                            'std': n_monthly.std()
                        }
                    }
                else:
                    # 步骤1: 计算每月的截面统计 (Cross-sectional statistics for each time period)
                    print(f"  - 计算 {var} 的月度截面统计...")

                    def calculate_cross_sectional_stats(group):
                        """计算单月的截面统计"""
                        data = group.dropna()
                        if len(data) == 0:
                            return pd.Series({
                                'count': 0,
                                'mean': np.nan,
                                'std': np.nan,
                                'min': np.nan,
                                'max': np.nan,
                                'skew': np.nan,
                                'kurt': np.nan,
                                **{f'p{int(p * 100)}': np.nan for p in extended_percentiles}
                            })

                        # 基础统计量
                        stats_dict = {
                            'count': len(data),
                            'mean': data.mean(),
                            'std': data.std(),
                            'min': data.min(),
                            'max': data.max(),
                            'skew': data.skew() if len(data) >= 3 else np.nan,
                            'kurt': data.kurtosis() if len(data) >= 4 else np.nan,
                        }

                        # 百分位数
                        for p in extended_percentiles:
                            stats_dict[f'p{int(p * 100)}'] = np.percentile(data, p * 100)

                        return pd.Series(stats_dict)

                    # 按年月分组计算截面统计
                    monthly_cross_sectional = self.final_stats.groupby(['year', 'month'])[var].apply(
                        calculate_cross_sectional_stats
                    ).unstack(fill_value=np.nan)

                    # 添加日期索引
                    monthly_cross_sectional.index = pd.to_datetime(
                        monthly_cross_sectional.index.get_level_values(0).astype(str) + '-' +
                        monthly_cross_sectional.index.get_level_values(1).astype(str) + '-01'
                    )

                    print(f"  - {var} 月度截面统计完成，时间序列长度: {len(monthly_cross_sectional)}")

                    # 步骤2: 计算截面统计的时间序列统计 (Time series statistics of cross-sectional statistics)
                    print(f"  - 计算 {var} 的时间序列统计...")

                    def safe_ts_stats(series):
                        """安全计算时间序列统计"""
                        clean_series = series.dropna()
                        if len(clean_series) == 0:
                            return {'mean': np.nan, 'std': np.nan, 'min': np.nan, 'max': np.nan, 'count': 0}
                        return {
                            'mean': clean_series.mean(),
                            'std': clean_series.std(),
                            'min': clean_series.min(),
                            'max': clean_series.max(),
                            'count': len(clean_series)
                        }

                    # 对每个截面统计量计算时间序列统计
                    ts_stats = {}
                    for col in monthly_cross_sectional.columns:
                        ts_stats[f'ts_{col}'] = safe_ts_stats(monthly_cross_sectional[col])

                    # 步骤3: 计算总体统计 (Overall statistics across all observations)
                    print(f"  - 计算 {var} 的总体统计...")
                    overall_data = self.final_stats[var].dropna()

                    overall_stats = {
                        'overall_count': len(overall_data),
                        'overall_mean': overall_data.mean(),
                        'overall_std': overall_data.std(),
                        'overall_min': overall_data.min(),
                        'overall_max': overall_data.max(),
                        'overall_skew': overall_data.skew() if len(overall_data) >= 3 else np.nan,
                        'overall_kurt': overall_data.kurtosis() if len(overall_data) >= 4 else np.nan,
                    }

                    # 总体百分位数
                    for p in extended_percentiles:
                        overall_stats[f'overall_p{int(p * 100)}'] = np.percentile(overall_data, p * 100)

                    # 合并所有统计结果
                    results[var] = {**ts_stats, **overall_stats}
                    detailed_results[var] = {
                        'monthly_cross_sectional': monthly_cross_sectional,
                        'time_series_stats': ts_stats,
                        'overall_stats': overall_stats
                    }

                    print(f"  - {var} 统计计算完成")

            self.results = results
            self.detailed_results = detailed_results

            # 生成汇总表
            print("正在生成汇总统计表...")
            self._generate_comprehensive_summary_table(variables, extended_percentiles)

            print("成功生成汇总表。")
            return True

        except Exception as e:
            print(f"计算描述性统计失败: {e}")
            traceback.print_exc()
            return False

    def _generate_comprehensive_summary_table(self, variables, percentiles):
        """生成综合汇总统计表"""
        try:
            summary_data = []

            for var in variables:
                stats = self.results[var]

                if var == 'n':
                    row = {
                        'Variable': var,
                        'Description': '每月截面数据个数',
                        'Time_Series_Length': stats['count'],
                        'Mean': f"{stats['mean']:.2f}",
                        'Std': f"{stats['std']:.2f}",
                        'Min': f"{stats['min']:.2f}",
                        'Max': f"{stats['max']:.2f}",
                        'P1': f"{stats['p1']:.2f}",
                        'P5': f"{stats['p5']:.2f}",
                        'P10': f"{stats['p10']:.2f}",
                        'P25': f"{stats['p25']:.2f}",
                        'P50': f"{stats['p50']:.2f}",
                        'P75': f"{stats['p75']:.2f}",
                        'P90': f"{stats['p90']:.2f}",
                        'P95': f"{stats['p95']:.2f}",
                        'P99': f"{stats['p99']:.2f}",
                        'CS_Mean_TS_Mean': 'N/A',
                        'CS_Mean_TS_Std': 'N/A',
                        'CS_Std_TS_Mean': 'N/A',
                        'CS_Std_TS_Std': 'N/A',
                        'CS_Skew_TS_Mean': 'N/A',
                        'CS_Kurt_TS_Mean': 'N/A'
                    }
                else:
                    # 获取关键的时间序列统计
                    cs_mean_ts = stats.get('ts_mean', {})
                    cs_std_ts = stats.get('ts_std', {})
                    cs_skew_ts = stats.get('ts_skew', {})
                    cs_kurt_ts = stats.get('ts_kurt', {})

                    row = {
                        'Variable': var,
                        'Description': self._get_variable_description(var),
                        'Time_Series_Length': cs_mean_ts.get('count', 0),
                        'Overall_Mean': f"{stats['overall_mean']:.6f}",
                        'Overall_Std': f"{stats['overall_std']:.6f}",
                        'Overall_Min': f"{stats['overall_min']:.6f}",
                        'Overall_Max': f"{stats['overall_max']:.6f}",
                        'Overall_Skew': f"{stats['overall_skew']:.4f}",
                        'Overall_Kurt': f"{stats['overall_kurt']:.4f}",
                        'Overall_Count': f"{stats['overall_count']:,}",

                        # 截面均值的时间序列统计
                        'CS_Mean_TS_Mean': f"{cs_mean_ts.get('mean', np.nan):.6f}",
                        'CS_Mean_TS_Std': f"{cs_mean_ts.get('std', np.nan):.6f}",
                        'CS_Mean_TS_Min': f"{cs_mean_ts.get('min', np.nan):.6f}",
                        'CS_Mean_TS_Max': f"{cs_mean_ts.get('max', np.nan):.6f}",

                        # 截面标准差的时间序列统计
                        'CS_Std_TS_Mean': f"{cs_std_ts.get('mean', np.nan):.6f}",
                        'CS_Std_TS_Std': f"{cs_std_ts.get('std', np.nan):.6f}",
                        'CS_Std_TS_Min': f"{cs_std_ts.get('min', np.nan):.6f}",
                        'CS_Std_TS_Max': f"{cs_std_ts.get('max', np.nan):.6f}",

                        # 截面偏度和峰度的时间序列均值
                        'CS_Skew_TS_Mean': f"{cs_skew_ts.get('mean', np.nan):.4f}",
                        'CS_Kurt_TS_Mean': f"{cs_kurt_ts.get('mean', np.nan):.4f}",
                    }

                    # 添加总体百分位数
                    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
                        row[f'Overall_P{p}'] = f"{stats[f'overall_p{p}']:.6f}"

                summary_data.append(row)

            self.summary_table = pd.DataFrame(summary_data)

            # 生成详细的百分位数分布表
            self._generate_percentile_distribution_table(variables, percentiles)

        except Exception as e:
            print(f"生成汇总表失败: {e}")
            traceback.print_exc()

    def _generate_percentile_distribution_table(self, variables, percentiles):
        """生成详细的百分位数分布表"""
        try:
            percentile_data = []

            for var in variables:
                if var == 'n':
                    continue

                stats = self.results[var]

                # 每个百分位数一行
                for p in percentiles:
                    p_int = int(p * 100)
                    percentile_data.append({
                        'Variable': var,
                        'Percentile': f'P{p_int}',
                        'Percentile_Value': p,
                        'Overall_Value': stats[f'overall_p{p_int}'],
                        'Description': f'{var}的第{p_int}百分位数'
                    })

            self.percentile_table = pd.DataFrame(percentile_data)

        except Exception as e:
            print(f"生成百分位数表失败: {e}")
            traceback.print_exc()

    def _get_variable_description(self, var):
        """获取变量描述"""
        descriptions = {
            'ret': '月度股票收益率',
            'size': '市值对数',
            'bm': '账面市值比',
            'mom': '动量指标(过去11个月收益率)'
        }
        return descriptions.get(var, var)

    def save_results(self, output_dir='/Users/jinanwuyanzu/Desktop/News 5'):
        """保存结果到CSV文件"""
        if self.summary_table is None:
            print("没有结果可以保存。")
            return False

        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)

        # 定义文件路径
        summary_path = os.path.join(output_dir, 'stock_descriptive_summary.csv')
        percentile_path = os.path.join(output_dir, 'stock_percentile_distribution.csv')
        sample_path = os.path.join(output_dir, 'stock_sample_data.csv')
        monthly_stats_path = os.path.join(output_dir, 'stock_monthly_cross_sectional_stats.csv')

        print(f"正在保存结果到 {output_dir}...")
        try:
            # 1. 保存主要汇总统计表
            self.summary_table.to_csv(summary_path, index=False, encoding='utf-8')
            print(f"✓ 主要汇总统计已保存到: {summary_path}")

            # 2. 保存百分位数分布表
            if hasattr(self, 'percentile_table') and self.percentile_table is not None:
                self.percentile_table.to_csv(percentile_path, index=False, encoding='utf-8')
                print(f"✓ 百分位数分布已保存到: {percentile_path}")

            # 3. 保存原始数据样本
            if self.final_stats is not None and not self.final_stats.empty:
                sample_size = min(1000, len(self.final_stats))
                sample_data = self.final_stats.sample(n=sample_size, random_state=42)
                sample_data.to_csv(sample_path, index=False, encoding='utf-8')
                print(f"✓ 样本数据已保存到: {sample_path}")

            # 4. 保存详细的月度截面统计（合并所有变量）
            if hasattr(self, 'detailed_results'):
                monthly_stats_combined = []

                for var in ['ret', 'size', 'bm', 'mom']:
                    if var in self.detailed_results and 'monthly_cross_sectional' in self.detailed_results[var]:
                        monthly_df = self.detailed_results[var]['monthly_cross_sectional'].copy()
                        monthly_df['variable'] = var
                        monthly_df['date'] = monthly_df.index
                        monthly_stats_combined.append(monthly_df)

                if monthly_stats_combined:
                    combined_monthly = pd.concat(monthly_stats_combined, ignore_index=True)
                    # 重新排列列的顺序
                    cols = ['variable', 'date'] + [col for col in combined_monthly.columns if
                                                   col not in ['variable', 'date']]
                    combined_monthly = combined_monthly[cols]
                    combined_monthly.to_csv(monthly_stats_path, index=False, encoding='utf-8')
                    print(f"✓ 月度截面统计已保存到: {monthly_stats_path}")

            # 5. 保存统计结果摘要报告
            self._save_summary_report(output_dir)

            return True
        except Exception as e:
            print(f"保存结果失败: {e}")
            traceback.print_exc()
            return False

    def _save_summary_report(self, output_dir):
        """保存统计结果摘要报告"""
        try:
            report_path = os.path.join(output_dir, 'analysis_summary_report.txt')

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("股票描述性统计分析报告\n")
                f.write("=" * 80 + "\n\n")

                # 数据概览
                f.write("1. 数据概览\n")
                f.write("-" * 40 + "\n")
                if self.final_stats is not None:
                    f.write(
                        f"分析时间范围: {self.final_stats['date'].min().strftime('%Y-%m')} 到 {self.final_stats['date'].max().strftime('%Y-%m')}\n")
                    f.write(f"总观测数: {len(self.final_stats):,}\n")
                    f.write(f"股票数量: {self.final_stats['permno'].nunique():,}\n")
                    f.write(f"平均每月股票数: {self.final_stats.groupby(['year', 'month']).size().mean():.0f}\n")
                f.write("\n")

                # 分析方法
                f.write("2. 分析方法\n")
                f.write("-" * 40 + "\n")
                f.write("采用Cross-sectional再Time series统计方法:\n")
                f.write("- 步骤1: 计算每月的截面统计量(平均值、标准差、百分位数等)\n")
                f.write("- 步骤2: 对截面统计量的时间序列计算平均值和标准差\n")
                f.write("- 步骤3: 报告总体分布的详细百分位数(1%-99%)\n")
                f.write("\n")

                # 核心发现
                f.write("3. 核心发现摘要\n")
                f.write("-" * 40 + "\n")

                if hasattr(self, 'results'):
                    for var in ['ret', 'size', 'bm', 'mom']:
                        if var in self.results:
                            stats = self.results[var]
                            f.write(f"\n{var.upper()} ({self._get_variable_description(var)}):\n")
                            f.write(f"  总体均值: {stats['overall_mean']:.6f}\n")
                            f.write(f"  总体标准差: {stats['overall_std']:.6f}\n")

                            ts_mean = stats.get('ts_mean', {})
                            ts_std = stats.get('ts_std', {})

                            f.write(f"  截面均值的时间序列均值: {ts_mean.get('mean', 'N/A')}\n")
                            f.write(f"  截面均值的时间序列标准差: {ts_mean.get('std', 'N/A')}\n")
                            f.write(f"  截面标准差的时间序列均值: {ts_std.get('mean', 'N/A')}\n")
                            f.write(f"  中位数(P50): {stats['overall_p50']:.6f}\n")
                            f.write(f"  极端值: P1={stats['overall_p1']:.6f}, P99={stats['overall_p99']:.6f}\n")

                f.write("\n")
                f.write("4. 输出文件说明\n")
                f.write("-" * 40 + "\n")
                f.write("- stock_descriptive_summary.csv: 主要汇总统计表\n")
                f.write("- stock_percentile_distribution.csv: 详细百分位数分布\n")
                f.write("- stock_monthly_cross_sectional_stats.csv: 月度截面统计时间序列\n")
                f.write("- stock_sample_data.csv: 原始数据样本\n")
                f.write("- analysis_summary_report.txt: 本分析报告\n")

            print(f"✓ 分析摘要报告已保存到: {report_path}")

        except Exception as e:
            print(f"保存摘要报告失败: {e}")
            traceback.print_exc()

    def run_analysis(self, start_date='1980-01-01', end_date='2024-12-31',
                     output_dir='/Users/jinanwuyanzu/Desktop/News 5'):
        """运行完整分析流程"""
        print("开始股票描述性统计分析...")
        print("=" * 50)
        start_time = datetime.now()

        try:
            if not self.connect_wrds():
                print("❌ 数据库连接失败")
                return False
            print("✓ 数据库连接成功")

            if not self.load_tickers():
                print("❌ 加载ticker失败")
                return False
            print("✓ ticker加载成功")

            if not self.get_stock_data(start_date, end_date):
                print("❌ 数据获取失败")
                return False
            print("✓ 数据获取成功")

            if not self.calculate_indicators():
                print("❌ 指标计算失败")
                return False
            print("✓ 指标计算成功")

            if not self.calculate_descriptive_stats():
                print("❌ 统计计算失败")
                return False
            print("✓ 统计计算成功")

            if not self.save_results(output_dir):
                print("❌ 结果保存失败")
                return False
            print("✓ 结果保存成功")

            # 显示结果
            print("\n" + "=" * 80)
            print("📊 描述性统计结果汇总")
            print("=" * 80)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(self.summary_table.to_string(index=False))

            return True

        except Exception as e:
            print(f"\n❌ 分析过程中出现严重错误: {e}")
            traceback.print_exc()
            return False
        finally:
            if self.db:
                self.db.close()
                print("\n🔌 数据库连接已关闭")
            end_time = datetime.now()
            print(f"\n⏱️ 总耗时: {end_time - start_time}")
            if hasattr(self, 'summary_table') and self.summary_table is not None:
                print(f"🎉 分析完成！结果已保存到: {output_dir}")
            else:
                print("⚠️ 分析未成功生成结果文件，请检查日志信息")


# ==================================
#            使用示例
# ==================================
if __name__ == "__main__":
    # 请将这里的路径修改为您自己存放 ticker_list.csv 文件的实际路径
    ticker_file_path = '../ticker list.csv'

    # 初始化分析器
    analyzer = StockDescriptiveStats(ticker_csv_path=ticker_file_path)

    # 运行分析，结果将保存到指定目录
    success = analyzer.run_analysis(
        start_date='1980-01-01',
        end_date='2024-12-31',
        output_dir='../News '
    )

    if success:
        print("\n✅ 所有分析步骤均已成功完成！")
        print("📁 输出文件说明:")
        print("  • stock_descriptive_summary.csv - 主要汇总统计表")
        print("  • stock_percentile_distribution.csv - 详细百分位数分布")
        print("  • stock_monthly_cross_sectional_stats.csv - 月度截面统计时间序列")
        print("  • stock_sample_data.csv - 原始数据样本")
        print("  • analysis_summary_report.txt - 分析摘要报告")
    else:
        print("\n❌ 分析过程中遇到问题，请查看上方错误信息")