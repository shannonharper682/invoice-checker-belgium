# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Invoice Checker Belgium v2
# MAGIC %md
# MAGIC # Invoice Checker Belgium v2
# MAGIC
# MAGIC Combined invoice checker merging three Belgium Fluxys invoice checks into a single notebook.
# MAGIC
# MAGIC **Checks performed:**
# MAGIC 1. **Long Term Capacity (LTC)** — Entry/Exit at IC Point (Firm + Interruptible) capacity bookings vs Endur deals
# MAGIC 2. **Allocation Settlement** — Purchases (bill) and Sales (self-bill) at end-user domestic points vs dispatch deltas
# MAGIC 3. **Variable Trading Fee** — ZTP Trading variable fee vs ZTPH shipper volumes
# MAGIC
# MAGIC **Usage:** Select an invoice XML file from the dropdown, then Run All.
# MAGIC
# MAGIC **Output tables** (schema: `ms_vulcan_gpgtopm_plab.combined_belgium_invoices`):
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `be_longterm_results` | LTC comparison — Entry/Exit at IC Point (Firm + Interruptible) |
# MAGIC | `be_longterm_flagged` | LTC flagged issues (capacity mismatches, back-billing) |
# MAGIC | `be_allocsettle_daily` | Allocation settlement daily comparison grid |
# MAGIC | `be_allocsettle_summary` | Allocation settlement monthly summary |
# MAGIC | `be_allocsettle_flagged` | Allocation settlement flagged days |
# MAGIC | `be_varfee_results` | Variable trading fee monthly results |
# MAGIC | `be_varfee_flags` | Variable trading fee daily flagged days |

# COMMAND ----------

# DBTITLE 1,Configuration & Widgets
# MAGIC %pip install openpyxl -q
# MAGIC
# MAGIC import os, re, calendar
# MAGIC import xml.etree.ElementTree as ET
# MAGIC import pandas as pd
# MAGIC import pyspark.sql.functions as F
# MAGIC from pyspark.sql.window import Window
# MAGIC from datetime import date, datetime, timedelta
# MAGIC
# MAGIC # =============================================================================
# MAGIC # FOLDERS
# MAGIC # =============================================================================
# MAGIC INVOICE_DIR  = "/Workspace/Users/shlh@equinor.com/invoice checking/belgium/invoices"
# MAGIC SELFBILL_DIR = "/Workspace/Users/shlh@equinor.com/invoice checking/belgium/self invoices"
# MAGIC
# MAGIC # =============================================================================
# MAGIC # DATA SOURCES
# MAGIC # =============================================================================
# MAGIC DISPATCH_TABLE     = "ms_atlas.dispatchnatgas_standard.hourly_v2latest"
# MAGIC PRICE_TABLE        = "ms_atlas.ms07splitprice_standard.eex_v1r0"
# MAGIC PRICE_DATA_PACKAGE = "Eex_EuropeanGasSpotIndex"
# MAGIC PRICE_KEYS         = "ZTP,Day"
# MAGIC PRICE_COLUMN       = "Index"
# MAGIC
# MAGIC # =============================================================================
# MAGIC # BELGIUM PARAMETERS
# MAGIC # =============================================================================
# MAGIC COUNTRY            = "BE"
# MAGIC END_USER_LOCATIONS = ["TESSENDERLO", "ANTWERP JUPITER"]
# MAGIC DOMESTIC_LOCATIONS = ["TESSENDERLO", "ANTWERP JUPITER"]
# MAGIC HUB_INTERNAL_LOCATIONS = ["ZBHUBEE", "ZBHUBFL"]
# MAGIC
# MAGIC # =============================================================================
# MAGIC # VARIABLE FEE PARAMETERS
# MAGIC # =============================================================================
# MAGIC MAPPING_PATH       = "/Workspace/Users/shlh@equinor.com/invoice checking/belgium/contract shipper code/mapping_new.xlsx"
# MAGIC QTY_MULTIPLIER     = 0.0008   # Energy in Cash (reference — not used in this notebook)
# MAGIC TRADING_FEE_RATE   = 0.00195  # EUR/MWh
# MAGIC
# MAGIC # =============================================================================
# MAGIC # OUTPUT SCHEMA & TABLES
# MAGIC # =============================================================================
# MAGIC NEW_SCHEMA = "ms_vulcan_gpgtopm_plab.combined_belgium_invoices"
# MAGIC
# MAGIC LTC_RESULTS_TABLE   = f"{NEW_SCHEMA}.be_longterm_results"
# MAGIC LTC_FLAGS_TABLE     = f"{NEW_SCHEMA}.be_longterm_flagged"
# MAGIC ALLOC_DAILY_TABLE   = f"{NEW_SCHEMA}.be_allocsettle_daily"
# MAGIC ALLOC_SUMMARY_TABLE = f"{NEW_SCHEMA}.be_allocsettle_summary"
# MAGIC ALLOC_FLAGS_TABLE   = f"{NEW_SCHEMA}.be_allocsettle_flagged"
# MAGIC VF_RESULTS_TABLE    = f"{NEW_SCHEMA}.be_varfee_results"
# MAGIC VF_FLAGS_TABLE      = f"{NEW_SCHEMA}.be_varfee_flags"
# MAGIC EIC_RESULTS_TABLE   = f"{NEW_SCHEMA}.be_eic_results"
# MAGIC EIC_FLAGS_TABLE     = f"{NEW_SCHEMA}.be_eic_flags"
# MAGIC
# MAGIC # =============================================================================
# MAGIC # GENERIC SAVE HELPER — DELETE+INSERT keyed on check_month
# MAGIC # =============================================================================
# MAGIC def _save_delta(pdf, table_fqn, check_month_val):
# MAGIC     """Delete-insert for this check_month, creating table if needed."""
# MAGIC     try:
# MAGIC         spark.sql(f"DELETE FROM {table_fqn} WHERE check_month = '{check_month_val}'")
# MAGIC         if len(pdf) > 0:
# MAGIC             spark.createDataFrame(pdf).write.mode("append").option("mergeSchema", "true").saveAsTable(table_fqn)
# MAGIC     except Exception as e:
# MAGIC         if 'TABLE_OR_VIEW_NOT_FOUND' in str(e) or 'DELTA_TABLE_NOT_FOUND' in str(e):
# MAGIC             if len(pdf) > 0:
# MAGIC                 spark.createDataFrame(pdf).write.option("mergeSchema", "true").saveAsTable(table_fqn)
# MAGIC             else:
# MAGIC                 print(f"  \u26a0\ufe0f {table_fqn} — no data and table doesn't exist yet")
# MAGIC                 return
# MAGIC         else:
# MAGIC             raise
# MAGIC     print(f"  \u2705 {table_fqn} — {len(pdf)} rows")
# MAGIC
# MAGIC # =============================================================================
# MAGIC # WIDGET — Invoice XML file selector
# MAGIC # =============================================================================
# MAGIC xml_files = sorted(f for f in os.listdir(INVOICE_DIR) if f.upper().endswith(".XML"))
# MAGIC assert len(xml_files) > 0, f"No XML files found in {INVOICE_DIR}"
# MAGIC dbutils.widgets.dropdown("invoice_file", xml_files[0], xml_files, "Invoice XML File")
# MAGIC INVOICE_FILE = dbutils.widgets.get("invoice_file")
# MAGIC INVOICE_PATH = os.path.join(INVOICE_DIR, INVOICE_FILE)
# MAGIC
# MAGIC print(f"Selected invoice: {INVOICE_FILE}")

# COMMAND ----------

# DBTITLE 1,Parse Invoice XML
# =============================================================================
# PARSER 1: LTC — Extract IC Point capacity lines (Firm + Interruptible)
# =============================================================================
def parse_invoice(path):
    tree = ET.parse(path)
    rows = []
    VALID = ('Entry at Interconnection Point (Firm)', 'Exit at Interconnection Point (Firm)',
             'Entry at Interconnection Point (Interruptible)', 'Exit at Interconnection Point (Interruptible)')
    for prod in tree.getroot().iter('CustomerProduct'):
        pname = (prod.findtext('ProductName') or '').strip()
        if not (pname.startswith(VALID) or 'Auction' in pname or 'Premium' in pname):
            continue
        _cap_type = 'Interruptible' if 'Interruptible' in pname else 'Firm'
        for il in prod.findall('./InvoiceLines/InvoiceLine'):
            for bq in il.findall('.//BilledQuantity'):
                ai, pfi = bq.find('AdditionalInformation'), bq.find('PriceFormulaInformation')
                qty_el = pfi.find('QTY') if pfi is not None else None
                up_el = pfi.find('UP') if pfi is not None else None
                econ_start = bq.findtext('EconomicStartDate')
                sh_start = int(ai.findtext('ServiceStartGasHour') or 0) if ai is not None else 0
                sh_end = int(ai.findtext('ServiceEndGasHour') or 0) if ai is not None else 0
                service_hours = (sh_end - sh_start + 1) if (sh_start > 0 and sh_end > 0) else 24
                if service_hours <= 0: service_hours += 24
                qty_val = float(qty_el.get('QTY', 0)) if qty_el is not None else None
                rows.append({
                    'capacity_type': _cap_type,
                    'invoice_line_group': (il.findtext('InvoiceLineGroup') or '').strip(),
                    'location': (il.findtext('InvoiceLine') or '').strip(),
                    'detail': (il.findtext('InvoiceLineDetail') or '').strip(),
                    'billing_start': pd.to_datetime(il.findtext('BillingStartDate')).date() if il.findtext('BillingStartDate') else None,
                    'gas_day': pd.to_datetime(econ_start).date() if econ_start else None,
                    'direction': ai.findtext('Direction') if ai is not None else None,
                    'service_rate_type': ai.findtext('ServiceRateType') if ai is not None else None,
                    'contract_ref': ai.findtext('ContractReference') if ai is not None else None,
                    'billed_amount': float(bq.findtext('BilledQuantityAmount') or 0),
                    'qty': qty_val,
                    'service_hours': service_hours,
                    'volume_kwh': qty_val * service_hours if qty_val else 0,
                    'up': float(up_el.get('UP', 0)) if up_el is not None else None,
                    'up_unit': up_el.get('UPUnit', '') if up_el is not None else None,
                })
    return pd.DataFrame(rows)

# =============================================================================
# PARSER 2: Generic — all product types (for Variable Fee)
# =============================================================================
def parse_invoice_xml(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    rows = []
    for product in root.findall('.//CustomerProduct'):
        product_name = product.find('ProductName').text
        for invoice_line in product.findall('.//InvoiceLine[@InvoiceLineSequence]'):
            line_group = invoice_line.find('InvoiceLineGroup')
            line_group = line_group.text if line_group is not None else None
            line_name_elem = invoice_line.find('InvoiceLine')
            line_name = line_name_elem.text if line_name_elem is not None else None
            for bq in invoice_line.findall('.//BilledQuantity'):
                econ_start = bq.find('EconomicStartDate').text
                amount = float(bq.find('BilledQuantityAmount').text)
                pfi = bq.find('PriceFormulaInformation')
                qty_elem = pfi.find('QTY')
                up_elem = pfi.find('UP')
                qty = float(qty_elem.get('QTY'))
                qty_unit = qty_elem.get('QTYUnit')
                up = float(up_elem.get('UP'))
                up_unit = up_elem.get('UPUnit')
                direction_elem = bq.find('AdditionalInformation/Direction')
                direction = direction_elem.text if direction_elem is not None else None
                rows.append({
                    'gas_day': econ_start, 'category': line_group,
                    'line_type': line_name, 'qty': qty, 'qty_unit': qty_unit,
                    'up': up, 'up_unit': up_unit, 'amount': amount,
                    'product_name': product_name, 'direction': direction
                })
    df = pd.DataFrame(rows)
    if len(df) > 0:
        df['gas_day'] = pd.to_datetime(df['gas_day']).dt.date
    return df

# =============================================================================
# PARSER 3: Allocation Settlement
# =============================================================================
def parse_alloc_settlement(xml_path, product_keyword):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    rows = []
    for product in root.findall('.//CustomerProduct'):
        pname = (product.find('ProductName').text or '').strip()
        if product_keyword.lower() not in pname.lower():
            continue
        if 'transm' in pname.lower() or 'imbal' in pname.lower():
            continue
        for il in product.findall('.//InvoiceLine[@InvoiceLineSequence]'):
            for bq in il.findall('.//BilledQuantity'):
                econ = bq.find('EconomicStartDate')
                amt_el = bq.find('BilledQuantityAmount')
                pfi = bq.find('PriceFormulaInformation')
                if econ is None or amt_el is None or pfi is None:
                    continue
                qty_el = pfi.find('QTY')
                up_el  = pfi.find('UP')
                rows.append({
                    'gas_day':    pd.to_datetime(econ.text).date(),
                    'qty_kwh':    float(qty_el.get('QTY')) if qty_el is not None else 0,
                    'up_eur_kwh': float(up_el.get('UP'))  if up_el  is not None else 0,
                    'amount_eur': float(amt_el.text),
                })
    return pd.DataFrame(rows)

def _get_alloc_period(xml_path, product_kw):
    """Return first day of billing period for allocation settlement product."""
    try:
        tree = ET.parse(xml_path)
        for prod in tree.getroot().findall('.//CustomerProduct'):
            pname = (prod.find('ProductName').text or '').strip()
            if product_kw.lower() not in pname.lower(): continue
            if 'transm' in pname.lower() or 'imbal' in pname.lower(): continue
            for il in prod.findall('.//InvoiceLine[@InvoiceLineSequence]'):
                bs = il.find('BillingStartDate')
                if bs is not None:
                    return pd.to_datetime(bs.text).date()
    except Exception:
        pass
    return None

# =============================================================================
# PARSE SELECTED INVOICE
# =============================================================================
print(f"{'='*80}")
print(f"INVOICE CHECKER BELGIUM v2 — {INVOICE_FILE}")
print(f"{'='*80}")

# --- 1. LTC parsing ---
df_all_ltc = parse_invoice(INVOICE_PATH)
billing_months = sorted(df_all_ltc['billing_start'].dropna().apply(lambda d: d.replace(day=1)).unique())
invoice_month = billing_months[-1]
med = calendar.monthrange(invoice_month.year, invoice_month.month)[1]
month_start, month_end = invoice_month, date(invoice_month.year, invoice_month.month, med)
all_month_start = billing_months[0]

IC = ['Entry at Interconnection Point - Firm', 'Exit at Interconnection Point - Firm',
      'Auction Premium on Exit at Interconnection Point - Firm',
      'Auction Premium on Entry at Interconnection Point - Firm',
      'Entry at Interconnection Point - Interruptible', 'Exit at Interconnection Point - Interruptible']
df_lt = df_all_ltc[df_all_ltc['invoice_line_group'].isin(IC)].copy()
df_lt['billing_month'] = df_lt['billing_start'].apply(lambda d: d.replace(day=1) if d else None)

is_auc = df_lt['detail'].str.contains('Auction|Premium', case=False, na=False)
def _contracts(x): return ', '.join(sorted(set(str(v) for v in x if v)))

summary = df_lt[~is_auc].groupby(['billing_month', 'direction', 'location', 'detail', 'service_rate_type', 'capacity_type'], as_index=False).agg(
    tariff_eur=('billed_amount', 'sum'), total_qty_kwh_h=('qty', 'sum'),
    total_volume_kwh=('volume_kwh', 'sum'), num_days=('gas_day', 'nunique'),
    up=('up', 'first'), up_unit=('up_unit', 'first'),
    contracts=('contract_ref', _contracts))
summary['capacity_kwh_h'] = summary['total_qty_kwh_h'] / summary['num_days']

auc_summary = (df_lt[is_auc].groupby(['billing_month', 'direction', 'location'], as_index=False)
    .agg(auction_eur=('billed_amount', 'sum'), contracts=('contract_ref', _contracts))
    if is_auc.any() else pd.DataFrame())

print(f"\n--- LONG TERM CAPACITY ---")
print(f"Invoice month (M): {invoice_month}  |  Previous (M-1): {billing_months[0] if len(billing_months) > 1 else 'N/A'}")
firm_count = len(df_lt[df_lt['capacity_type'] == 'Firm'])
int_count = len(df_lt[df_lt['capacity_type'] == 'Interruptible'])
print(f"IC capacity lines: {(~is_auc).sum()} tariff + {is_auc.sum()} auction  (Firm: {firm_count}, Interruptible: {int_count})")
for bm in billing_months:
    bm_rows = summary[summary['billing_month'] == bm]
    label = 'M' if bm == invoice_month else 'M-1'
    for ct in sorted(bm_rows['capacity_type'].unique()):
        ct_rows = bm_rows[bm_rows['capacity_type'] == ct]
        lt_count = len(ct_rows[ct_rows['service_rate_type'] == 'LongTerm'])
        seas_count = len(ct_rows[ct_rows['service_rate_type'] == 'Season'])
        print(f"  {bm} ({label}) [{ct}]: {lt_count} long-term + {seas_count} seasonal groups")

# --- 2. Variable Fee & Energy in Cash parsing ---
# Both are billed ~3 months in arrears — detect period from actual gas_days
df_all_vf = parse_invoice_xml(INVOICE_PATH)

# Identify all arrears entries: Variable Fee (ZTP Trading) + Energy in Cash
_is_trading_vf = (df_all_vf['line_type'] == 'Variable Fee') & (df_all_vf['category'] == 'ZTP Trading')
_is_eic = df_all_vf['line_type'] == 'Energy in Cash'
_arrears_mask = _is_trading_vf | _is_eic
_arrears = df_all_vf[_arrears_mask]

if len(_arrears) > 0:
    _arr_min = _arrears['gas_day'].min()
    ARREARS_MONTH_START = _arr_min.replace(day=1) if hasattr(_arr_min, 'replace') else date(_arr_min.year, _arr_min.month, 1)
    _arr_med = calendar.monthrange(ARREARS_MONTH_START.year, ARREARS_MONTH_START.month)[1]
    ARREARS_MONTH_END = date(ARREARS_MONTH_START.year, ARREARS_MONTH_START.month, _arr_med)
    ARREARS_CHECK_MONTH = f"{ARREARS_MONTH_START.year:04d}-{ARREARS_MONTH_START.month:02d}"
else:
    ARREARS_MONTH_START = invoice_month
    ARREARS_MONTH_END = month_end
    ARREARS_CHECK_MONTH = f"{invoice_month.year:04d}-{invoice_month.month:02d}"

# Extract Variable Fee (ZTP Trading)
vf = df_all_vf[
    _is_trading_vf &
    (df_all_vf['gas_day'] >= ARREARS_MONTH_START) &
    (df_all_vf['gas_day'] <= ARREARS_MONTH_END)
].copy()

# Extract Energy in Cash (all categories)
eic = df_all_vf[
    _is_eic &
    (df_all_vf['gas_day'] >= ARREARS_MONTH_START) &
    (df_all_vf['gas_day'] <= ARREARS_MONTH_END)
].copy()

# Update shared variables for downstream VF cells
CHECK_MONTH = ARREARS_CHECK_MONTH
MONTH_START = ARREARS_MONTH_START
MONTH_END   = ARREARS_MONTH_END

print(f"\n--- VARIABLE FEE & ENERGY IN CASH (arrears period: {ARREARS_CHECK_MONTH}) ---")
print(f"Arrears period: {ARREARS_MONTH_START} to {ARREARS_MONTH_END}")
print(f"  Variable Fee (ZTP Trading): {len(vf)} daily entries")
print(f"  Energy in Cash:             {len(eic)} daily entries")

# --- 3. Allocation Settlement detection ---
settle_start_date = _get_alloc_period(INVOICE_PATH, 'Allocation Settlement')
if settle_start_date:
    SETTLE_MONTH_LABEL = f"{calendar.month_name[settle_start_date.month]} {settle_start_date.year}"
    SETTLE_START = settle_start_date
    SETTLE_END = date(settle_start_date.year, settle_start_date.month,
                      calendar.monthrange(settle_start_date.year, settle_start_date.month)[1])
    SETTLE_CK = f"{SETTLE_START.year}-{SETTLE_START.month:02d}"
    # Find matching self-bill XML
    _seen_inv = set()
    SELFBILL_PATH = None
    for f in sorted(os.listdir(SELFBILL_DIR)):
        if not f.upper().endswith('.XML'): continue
        inv_match = re.search(r'(\d{10})', f)
        inv_num = inv_match.group(1) if inv_match else f
        if inv_num in _seen_inv: continue
        _seen_inv.add(inv_num)
        start = _get_alloc_period(os.path.join(SELFBILL_DIR, f), 'Allocation Settl')
        if start and start.replace(day=1) == SETTLE_START.replace(day=1):
            SELFBILL_PATH = os.path.join(SELFBILL_DIR, f)
            break
    print(f"\n--- ALLOCATION SETTLEMENT ---")
    print(f"Settlement month: {SETTLE_MONTH_LABEL}  ({SETTLE_START} to {SETTLE_END})")
    print(f"Bill: {INVOICE_FILE}")
    if SELFBILL_PATH:
        print(f"Self-bill: {os.path.basename(SELFBILL_PATH)}")
    else:
        print(f"Self-bill: None — no sales settlement this month")
    HAS_ALLOC = True
else:
    print(f"\n--- ALLOCATION SETTLEMENT ---")
    print(f"No allocation settlement entries in this invoice — skipping")
    HAS_ALLOC = False
    SETTLE_START = SETTLE_END = SETTLE_CK = SETTLE_MONTH_LABEL = SELFBILL_PATH = None

# =============================================================================
# SUMMARY TABLES — what we extracted from the invoice
# =============================================================================
print(f"\n{'='*80}")
print("EXTRACTION SUMMARY")
print(f"{'='*80}")

# --- LTC Summary ---
print(f"\n\u2501\u2501 LONG TERM CAPACITY ({invoice_month.strftime('%B %Y')}) \u2501\u2501")
ltc_summary_display = summary[['billing_month', 'capacity_type', 'direction', 'location', 'service_rate_type',
    'capacity_kwh_h', 'tariff_eur', 'num_days', 'up', 'up_unit']].copy()
ltc_summary_display.columns = ['Billing Month', 'Capacity Type', 'Direction', 'Location', 'Rate Type',
    'Capacity kWh/h', 'Tariff EUR', 'Days', 'Unit Price', 'Unit']
if len(auc_summary) > 0:
    auc_display = auc_summary[['billing_month', 'direction', 'location', 'auction_eur']].copy()
    auc_display.columns = ['Billing Month', 'Direction', 'Location', 'Auction EUR']
    print("\nTariff lines:")
    display(ltc_summary_display)
    print("Auction premiums:")
    display(auc_display)
else:
    display(ltc_summary_display)

# --- Variable Fee Summary ---
print(f"\n\u2501\u2501 VARIABLE TRADING FEE ({ARREARS_CHECK_MONTH}) \u2501\u2501")
if len(vf) > 0:
    vf_summary = vf.groupby(['direction'], as_index=False).agg(
        days=('gas_day', 'nunique'),
        total_qty=('qty', 'sum'),
        total_amount=('amount', 'sum'),
        avg_price=('up', 'mean')
    )
    vf_summary.columns = ['Direction', 'Days', 'Total Qty', 'Total EUR', 'Avg Price']
    display(vf_summary)
else:
    print("  No Variable Fee entries")

# --- Energy in Cash Summary ---
print(f"\n\u2501\u2501 ENERGY IN CASH ({ARREARS_CHECK_MONTH}) \u2501\u2501")
if len(eic) > 0:
    eic_summary = eic.groupby(['category', 'direction'], as_index=False).agg(
        days=('gas_day', 'nunique'),
        total_qty=('qty', 'sum'),
        total_amount=('amount', 'sum'),
        avg_price=('up', 'mean')
    )
    eic_summary.columns = ['Category', 'Direction', 'Days', 'Total Qty', 'Total EUR', 'Avg Price']
    display(eic_summary)
else:
    print("  No Energy in Cash entries")

# --- Allocation Settlement Summary ---
print(f"\n\u2501\u2501 ALLOCATION SETTLEMENT ({SETTLE_MONTH_LABEL if HAS_ALLOC else 'N/A'}) \u2501\u2501")
if HAS_ALLOC:
    df_bill = parse_alloc_settlement(INVOICE_PATH, 'Allocation Settlement')
    if SELFBILL_PATH:
        df_sb = parse_alloc_settlement(SELFBILL_PATH, 'Allocation Settl')
    else:
        df_sb = pd.DataFrame()
    alloc_overview = []
    if len(df_bill) > 0:
        alloc_overview.append({
            'Source': f'Bill ({INVOICE_FILE})',
            'Type': 'Purchases', 'Days': df_bill['gas_day'].nunique(),
            'Total Qty kWh': f"{df_bill['qty_kwh'].sum():,.0f}",
            'Total EUR': f"{df_bill['amount_eur'].sum():,.2f}"
        })
    if len(df_sb) > 0:
        alloc_overview.append({
            'Source': f'Self-bill ({os.path.basename(SELFBILL_PATH)})',
            'Type': 'Sales', 'Days': df_sb['gas_day'].nunique(),
            'Total Qty kWh': f"{df_sb['qty_kwh'].sum():,.0f}",
            'Total EUR': f"{df_sb['amount_eur'].sum():,.2f}"
        })
    if alloc_overview:
        display(pd.DataFrame(alloc_overview))
    else:
        print("  No allocation settlement data parsed")
else:
    print("  No allocation settlement in this invoice")

print(f"\n{'='*80}")

# COMMAND ----------

# DBTITLE 1,LTC — Endur Pull
# =============================================================================
# LONG TERM CAPACITY — ENDUR PULL
# =============================================================================

ROUTE_MAP = {
    ('Entry', 'ZPT'):         {'endur': ['ZBEE Entry'],         'type': 'seasonal'},
    ('Entry', 'Zeebrugge'):   {'endur': ['ZBHUBEE-N-X'],        'type': 'seasonal'},
    ('Entry', 'VIP THE-ZTP'): {'endur': ['VIP THE-ZTP BE'],     'type': 'seasonal'},
    ('Exit',  'VIP BENE'):    {'endur': ['VIP_BENE_FLX-N-X'],   'type': 'longterm'},
    ('Exit',  'VIP THE-ZTP'): {'endur': ['VIP THE-ZTP BE'],     'type': 'longterm'},
    ('Exit',  'Virtualys'):   {'endur': ['VIRTUALYS (BL)-N-X'], 'type': 'longterm'},
}

# Seasonal window — derived from invoice gas_days
_seas_days = df_lt[df_lt['service_rate_type'] == 'Season']['gas_day'].dropna()
_prev_month_1st = (invoice_month.replace(day=1) - timedelta(days=1)).replace(day=1)
_next_month_1st = (invoice_month.replace(day=28) + timedelta(days=4)).replace(day=1)

if len(_seas_days) > 0:
    _in_window = _seas_days[(_seas_days >= _prev_month_1st) & (_seas_days <= _next_month_1st)]
    seasonal_window_start = _in_window.min() if len(_in_window) > 0 else _prev_month_1st.replace(day=10)
    seasonal_window_end = _in_window.max() if len(_in_window) > 0 else invoice_month.replace(day=9)
    _correction_days = sorted(_seas_days[(_seas_days < _prev_month_1st) | (_seas_days > _next_month_1st)].unique())
else:
    seasonal_window_start = _prev_month_1st.replace(day=10)
    seasonal_window_end = invoice_month.replace(day=9)
    _correction_days = []

_sql_start = min(all_month_start, seasonal_window_start) if len(_seas_days) > 0 else all_month_start
if _correction_days:
    _sql_start = min(_sql_start, min(_correction_days))

print(f"Seasonal window: {seasonal_window_start} to {seasonal_window_end}")
if _correction_days:
    print(f"Correction days: {_correction_days}")

df_endur = spark.sql(f"""
    WITH net_billed AS (
        SELECT DISTINCT pv2.location_id
        FROM ms_atlas.endur_standard.deal_v2latest d2
        JOIN ms_atlas.endur_standard.profilevolume_v2latest pv2 ON d2.deal_number = pv2.deal_number
        WHERE d2.instrument_type_name IN ('COMM-CAP-EXIT','COMM-CAP-ENTRY')
          AND d2.buy_sell_name='Sell' AND d2.service_type IN ('Firm','Interruptible') AND d2.tran_status='Validated'
          AND pv2.settlement_type_id=1 AND pv2.price>0 AND LOWER(d2.reference) NOT LIKE '%conversion%'
    ),
    raw AS (
        SELECT d.deal_number, d.reference, d.service_type AS endur_capacity_type,
            CASE WHEN d.instrument_type_name='COMM-CAP-ENTRY' THEN 'Entry' ELSE 'Exit' END AS direction,
            CASE WHEN d.reference LIKE 'PRI-%' THEN SPLIT(d.reference,'-')[2] ELSE NULL END AS allocation_id,
            l.location_name,
            CAST(DATE_TRUNC('month', pv.start_date) AS DATE) AS billing_month,
            pv.calculated_profile_volume_kwh AS vol,
            pv.price,
            pv.calculated_profile_volume_kwh / ((DATEDIFF(pv.end_date,pv.start_date)+1)*24) AS capacity_kwh_h,
            pv.calculated_profile_volume_kwh * pv.price AS endur_eur,
            pv.start_date AS pv_start,
            CASE
                WHEN DATEDIFF(d.deal_end_date, d.deal_start_date) >= 360 THEN 'Yearly'
                WHEN DATEDIFF(d.deal_end_date, d.deal_start_date) >= 85 THEN 'Quarterly'
                WHEN DATEDIFF(d.deal_end_date, d.deal_start_date) >= 27 THEN 'Monthly'
                ELSE 'Daily'
            END AS booking_type,
            ROW_NUMBER() OVER (PARTITION BY d.deal_number, pv.start_date ORDER BY pv.price DESC) AS rn
        FROM ms_atlas.endur_standard.deal_v2latest d
        JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number=pv.deal_number
        JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id=l.location_id
        JOIN ms_vulcan_gpgtopm_plab.hub2hub_v1.location_country_mapping cm ON pv.location_id=cm.location_id
        WHERE d.instrument_type_name IN ('COMM-CAP-EXIT','COMM-CAP-ENTRY')
          AND d.service_type IN ('Firm','Interruptible') AND d.tran_status='Validated' AND pv.settlement_type_id=1
          AND (pv.price!=0 OR (d.reference LIKE 'PRI_SEC_%' AND d.buy_sell_name='Sell'
               AND pv.location_id IN (SELECT location_id FROM net_billed)))
          AND cm.country_code='BE'
          AND pv.start_date >= '{_sql_start}' AND pv.end_date <= '{month_end}'
    )
    SELECT * FROM raw WHERE rn=1
""")

df_longterm = df_endur.filter(F.col('booking_type') != 'Daily')
df_seasonal = df_endur.filter(
    (F.col('booking_type') == 'Daily') &
    (
        ((F.col('pv_start').cast('date') >= F.lit(str(seasonal_window_start))) &
         (F.col('pv_start').cast('date') <= F.lit(str(seasonal_window_end))))
        | (F.col('pv_start').cast('date').isin([str(d) for d in _correction_days]) if _correction_days else F.lit(False))
    )
)

lt_count = df_longterm.count()
seas_count = df_seasonal.count()
print(f"\nEndur deals ({_sql_start} to {month_end}): {lt_count} long-term + {seas_count} seasonal")
for bm in billing_months:
    lt_ct = df_longterm.filter(F.col('billing_month') == str(bm)).count()
    seas_ct = df_seasonal.filter(F.col('billing_month') == str(bm)).count()
    label = 'M' if bm == invoice_month else 'M-1'
    # Show Firm vs Interruptible breakdown
    lt_firm = df_longterm.filter((F.col('billing_month') == str(bm)) & (F.col('endur_capacity_type') == 'Firm')).count()
    lt_int = df_longterm.filter((F.col('billing_month') == str(bm)) & (F.col('endur_capacity_type') == 'Interruptible')).count()
    seas_firm = df_seasonal.filter((F.col('billing_month') == str(bm)) & (F.col('endur_capacity_type') == 'Firm')).count()
    seas_int = df_seasonal.filter((F.col('billing_month') == str(bm)) & (F.col('endur_capacity_type') == 'Interruptible')).count()
    print(f"  {bm} ({label}): {lt_ct} long-term (F:{lt_firm} I:{lt_int}) + {seas_ct} seasonal (F:{seas_firm} I:{seas_int})")

print(f"\n--- Long-term deals ---")
display(df_longterm.select('deal_number','reference','direction','location_name','billing_month','booking_type','endur_capacity_type','vol','price','capacity_kwh_h','endur_eur')
    .orderBy('billing_month','direction','location_name'))
if seas_count > 0:
    print(f"\n--- Seasonal deals (sample) ---")
    display(df_seasonal.select('deal_number','reference','direction','location_name','pv_start','booking_type','endur_capacity_type','vol','price','capacity_kwh_h','endur_eur')
        .orderBy('pv_start','direction','location_name').limit(20))

# COMMAND ----------

# DBTITLE 1,LTC — Compare & Flag
# =============================================================================
# LONG TERM CAPACITY — COMPARE INVOICE vs ENDUR
# =============================================================================
compare_base = (
    df_lt[~is_auc]
    .groupby(['billing_month', 'direction', 'location', 'service_rate_type', 'capacity_type'], as_index=False)
    .agg(
        tariff_eur=('billed_amount', 'sum'),
        total_qty_kwh_h=('qty', 'sum'),
        total_volume_kwh=('volume_kwh', 'sum'),
        num_days=('gas_day', 'nunique'),
        invoice_up=('up', 'first'),
        invoice_up_unit=('up_unit', 'first'),
        detail=('detail', lambda x: ' | '.join(sorted(set(str(v) for v in x if v)))),
        contracts=('contract_ref', _contracts),
    )
)
compare_base['invoice_kwh_h'] = compare_base['total_qty_kwh_h'] / compare_base['num_days']

rows = []
annualized_price_col = F.when(
    F.abs(F.col('price')) >= F.lit(0.01),
    F.abs(F.col('price')) * F.lit(24 * 365 / 1000.0)
).otherwise(
    F.abs(F.col('price')) * F.lit(24 * 365.0)
)

for _, r in compare_base.iterrows():
    bm, d, loc = r['billing_month'], r['direction'], r['location']
    rate_type = r['service_rate_type']
    cap_type = r['capacity_type']
    cfg = ROUTE_MAP.get((d, loc))
    endur_locs = cfg['endur'] if cfg else None
    endur_df = df_longterm if rate_type == 'LongTerm' else df_seasonal
    # Filter Endur to matching capacity type (Firm/Interruptible)
    if endur_df is not None:
        endur_df = endur_df.filter(F.col('endur_capacity_type') == cap_type)
    days_in_month = calendar.monthrange(bm.year, bm.month)[1]

    e_cap = e_eur = endur_price = None
    if endur_locs:
        endur_rows = endur_df.filter(
            (F.col('location_name').isin(endur_locs)) &
            (F.col('billing_month') == str(bm))
        )
        if rate_type == 'LongTerm':
            agg = endur_rows.agg(
                F.abs(F.sum(F.col('capacity_kwh_h'))).alias('cap_sum'),
                (
                    F.sum(F.abs(F.col('capacity_kwh_h')) * annualized_price_col)
                    / F.sum(F.abs(F.col('capacity_kwh_h')))
                ).alias('endur_price')
            ).collect()[0]
            if agg['cap_sum']:
                e_cap = float(agg['cap_sum'])
                endur_price = float(agg['endur_price']) if agg['endur_price'] is not None else None
                if endur_price is not None:
                    e_eur = e_cap * endur_price * days_in_month / 365
        else:
            agg = endur_rows.agg(
                F.countDistinct(F.col('pv_start').cast('date')).alias('distinct_days'),
                F.sum(F.abs(F.col('capacity_kwh_h'))).alias('cap_sum'),
                F.sum(F.abs(F.col('vol'))).alias('vol_sum'),
                F.sum(F.abs(F.col('endur_eur'))).alias('eur_sum')
            ).collect()[0]
            if agg['distinct_days']:
                e_cap = float(agg['cap_sum']) / float(agg['distinct_days']) if agg['cap_sum'] is not None else None
                e_eur = float(agg['eur_sum']) if agg['eur_sum'] is not None else None
                endur_price = (float(agg['eur_sum']) / float(agg['vol_sum'])) if agg['vol_sum'] else None

    invoice_price = (
        float(r['invoice_up']) if rate_type == 'LongTerm' else
        (float(r['tariff_eur']) / float(r['total_volume_kwh']) if r['total_volume_kwh'] else None)
    )
    price_unit = r['invoice_up_unit'] if rate_type == 'LongTerm' else 'EUR/kWh'
    price_gap = (endur_price - invoice_price) if (endur_price is not None and invoice_price is not None) else None
    price_gap_pct = (price_gap / invoice_price * 100) if (price_gap is not None and invoice_price) else None

    auc_eur = 0.0
    if len(auc_summary) > 0:
        t_aucs = set(re.findall(r'AUC-(\d+)', r['contracts']))
        for _, a in auc_summary.iterrows():
            if a['billing_month'] == bm and a['direction'] == d and t_aucs & set(re.findall(r'AUC-(\d+)', a.get('contracts',''))):
                auc_eur += a['auction_eur']

    eq_eur = e_eur
    if auc_eur > 0 and rate_type == 'LongTerm':
        eq_eur = r['invoice_kwh_h'] * invoice_price * days_in_month / 365 if invoice_price is not None else None
        endur_price = invoice_price
        price_gap = 0.0 if invoice_price is not None else None
        price_gap_pct = 0.0 if invoice_price is not None else None
    rows.append({
        'billing_month': str(bm), 'direction': d, 'location': loc, 'line_type': 'Tariff',
        'capacity_type': cap_type, 'service_rate_type': rate_type, 'detail': r['detail'],
        'invoice_kwh_h': round(r['invoice_kwh_h'], 2),
        'endur_kwh_h': round(e_cap, 2) if e_cap is not None else None,
        'invoice_eur': round(r['tariff_eur'], 2),
        'equinor_eur': round(eq_eur, 2) if eq_eur is not None else None,
        'gap_eur': round(eq_eur - r['tariff_eur'], 2) if eq_eur is not None else None,
        'invoice_price': round(invoice_price, 9) if invoice_price is not None else None,
        'endur_price': round(endur_price, 9) if endur_price is not None else None,
        'price_gap': round(price_gap, 9) if price_gap is not None else None,
        'price_gap_pct': round(price_gap_pct, 3) if price_gap_pct is not None else None,
        'price_unit': price_unit, 'tariff': r['invoice_up'], 'tariff_unit': r['invoice_up_unit'],
    })
    if auc_eur > 0:
        base_tariff_eur = None
        if invoice_price is not None:
            base_tariff_eur = r['invoice_kwh_h'] * invoice_price * days_in_month / 365 if rate_type == 'LongTerm' else r['total_volume_kwh'] * invoice_price
        auc_endur = (e_eur - base_tariff_eur) if (e_eur is not None and base_tariff_eur is not None) else None
        rows.append({
            'billing_month': str(bm), 'direction': d, 'location': loc, 'line_type': 'Auction Premium',
            'capacity_type': cap_type, 'service_rate_type': rate_type, 'detail': 'Auction Premium',
            'invoice_kwh_h': None, 'endur_kwh_h': None,
            'invoice_eur': round(auc_eur, 2),
            'equinor_eur': round(auc_endur, 2) if auc_endur is not None else None,
            'gap_eur': round(auc_endur - auc_eur, 2) if auc_endur is not None else None,
            'invoice_price': None, 'endur_price': None, 'price_gap': None, 'price_gap_pct': None,
            'price_unit': None, 'tariff': None, 'tariff_unit': None,
        })

df_comparison = pd.DataFrame(rows)
for bm in billing_months:
    label = 'M' if bm == invoice_month else 'M-1'
    bm_df = df_comparison[df_comparison['billing_month'] == str(bm)]
    if len(bm_df) > 0:
        print(f"\nCapacity Summary \u2014 {bm.strftime('%B %Y')} ({label})")
        display(bm_df)

# =============================================================================
# FLAG ISSUES
# =============================================================================
flags = []
for _, r in df_comparison.iterrows():
    bm, d, loc, lt = r['billing_month'], r['direction'], r['location'], r['line_type']
    rate_type = r.get('service_rate_type', 'LongTerm')
    gas_day = bm
    if lt == 'Tariff':
        if r['endur_kwh_h'] is None or pd.isna(r['endur_kwh_h']):
            flags.append({'type': rate_type, 'capacity_type': r.get('capacity_type', 'Firm'),
                'gas_day': gas_day, 'direction': d, 'location': loc,
                'invoice_value': r['invoice_kwh_h'], 'endur_value': None,
                'gap': None, 'gap_pct': None, 'unit': 'kWh/h', 'note': f'NO ENDUR MATCH [{r.get("capacity_type", "Firm")}]'})
        else:
            if r['endur_kwh_h'] != r['invoice_kwh_h']:
                gap = r['endur_kwh_h'] - r['invoice_kwh_h']
                gap_pct = round(gap / r['invoice_kwh_h'] * 100, 3) if r['invoice_kwh_h'] else None
                if gap_pct is not None and abs(gap_pct) > 1:
                    flags.append({'type': rate_type, 'capacity_type': r.get('capacity_type', 'Firm'),
                        'gas_day': gas_day, 'direction': d, 'location': loc,
                        'invoice_value': r['invoice_kwh_h'], 'endur_value': r['endur_kwh_h'],
                        'gap': gap, 'gap_pct': gap_pct, 'unit': 'kWh/h', 'note': 'capacity rate mismatch'})
            if r.get('price_gap_pct') is not None and abs(r['price_gap_pct']) > 1:
                flags.append({'type': rate_type, 'capacity_type': r.get('capacity_type', 'Firm'),
                    'gas_day': gas_day, 'direction': d, 'location': loc,
                    'invoice_value': r.get('invoice_price'), 'endur_value': r.get('endur_price'),
                    'gap': r.get('price_gap'), 'gap_pct': r.get('price_gap_pct'),
                    'unit': r.get('price_unit'), 'note': 'price mismatch'})
    elif lt == 'Auction Premium':
        if r['gap_eur'] is not None and r['invoice_eur']:
            gap_pct = round(r['gap_eur'] / r['invoice_eur'] * 100, 3)
            if abs(gap_pct) > 1:
                flags.append({'type': 'AuctionPremium', 'capacity_type': r.get('capacity_type', 'Firm'),
                    'gas_day': gas_day, 'direction': d, 'location': loc,
                    'invoice_value': r['invoice_eur'], 'endur_value': r['equinor_eur'],
                    'gap': r['gap_eur'], 'gap_pct': gap_pct, 'unit': 'EUR', 'note': 'auction premium EUR mismatch'})

# Endur-only routes
for bm in billing_months:
    inv_routes = set((r['direction'], r['location'], r['capacity_type']) for _, r in compare_base[compare_base['billing_month'] == bm].iterrows())
    for (d, loc), cfg in ROUTE_MAP.items():
        if bm != invoice_month and cfg['type'] == 'longterm': continue
        for _ect in ['Firm', 'Interruptible']:
            if (d, loc, _ect) in inv_routes: continue
            endur_locs = cfg['endur']
            endur_df = df_longterm if cfg['type'] == 'longterm' else df_seasonal
            endur_df = endur_df.filter(F.col('endur_capacity_type') == _ect)
            agg = endur_df.filter((F.col('location_name').isin(endur_locs)) & (F.col('billing_month') == str(bm))).agg(
            F.abs(F.sum(F.col('capacity_kwh_h'))).alias('cap_net'),
            F.sum(F.abs(F.col('capacity_kwh_h'))).alias('cap_sum'),
            F.countDistinct(F.col('pv_start').cast('date')).alias('distinct_days')
        ).collect()[0]
            endur_cap = None
            if agg['cap_net'] or agg['cap_sum']:
                endur_cap = float(agg['cap_net']) if cfg['type'] == 'longterm' else (
                    float(agg['cap_sum']) / float(agg['distinct_days']) if agg['distinct_days'] else None)
            if endur_cap and endur_cap > 0:
                flags.append({'type': 'LongTerm' if cfg['type'] == 'longterm' else 'Season',
                    'capacity_type': _ect,
                    'gas_day': str(bm), 'direction': d, 'location': loc,
                    'invoice_value': None, 'endur_value': endur_cap,
                    'gap': None, 'gap_pct': None, 'unit': 'kWh/h', 'note': f'ENDUR ONLY [{_ect}] (not invoiced)'})

ltc_flags = pd.DataFrame(flags)
if len(ltc_flags) > 0:
    print(f"\n{len(ltc_flags)} LTC issue(s) flagged:")
    display(ltc_flags.sort_values(['type', 'direction', 'location', 'gas_day', 'note']))
else:
    print("\n\u2705 LTC: No issues \u2014 all routes match perfectly.")

# COMMAND ----------

# DBTITLE 1,LTC — Save
# =============================================================================
# LONG TERM CAPACITY — SAVE TO DELTA
# =============================================================================
run_ts = datetime.now()

# --- Results ---
df_ltc_save = df_comparison.copy()
df_ltc_save['check_month'] = str(invoice_month)
df_ltc_save['invoice_file'] = INVOICE_FILE
df_ltc_save['run_timestamp'] = run_ts
# Add period label: Capacity M vs Capacity M-1
df_ltc_save['period_label'] = df_ltc_save['billing_month'].apply(
    lambda bm: 'Capacity M' if bm == str(invoice_month) else 'Capacity M-1'
)

print("Saving LTC results:")
_save_delta(df_ltc_save, LTC_RESULTS_TABLE, str(invoice_month))

# --- Flags ---
if len(ltc_flags) > 0:
    df_ltc_flags_save = ltc_flags.copy()
    df_ltc_flags_save['check_month'] = str(invoice_month)
    df_ltc_flags_save['invoice_file'] = INVOICE_FILE
    df_ltc_flags_save['run_timestamp'] = run_ts
else:
    df_ltc_flags_save = pd.DataFrame()

_save_delta(df_ltc_flags_save, LTC_FLAGS_TABLE, str(invoice_month))
print(f"\nLTC save complete.")

# COMMAND ----------

# DBTITLE 1,Allocation Settlement — Parse & Dispatch
# =============================================================================
# ALLOCATION SETTLEMENT — PARSE INVOICES & PULL DISPATCH + PRICES
# =============================================================================

if not HAS_ALLOC:
    print("Allocation settlement skipped — no entries in this invoice.")
    alloc_comp = pd.DataFrame()
    alloc_flagged = pd.DataFrame()
    alloc_summary_rows = []
    has_final = False
else:
    # --- Parse bill and self-bill ---
    df_bill_raw = parse_alloc_settlement(INVOICE_PATH, 'Allocation Settlement') if INVOICE_PATH else pd.DataFrame()
    df_sb_raw   = parse_alloc_settlement(SELFBILL_PATH, 'Allocation Settl') if SELFBILL_PATH else pd.DataFrame()

    if len(df_bill_raw) > 0:
        df_bill_daily = df_bill_raw.groupby('gas_day', as_index=False).agg(
            bill_qty_kwh=('qty_kwh', 'sum'), bill_up=('up_eur_kwh', 'first'), bill_eur=('amount_eur', 'sum'))
    else:
        df_bill_daily = pd.DataFrame(columns=['gas_day', 'bill_qty_kwh', 'bill_up', 'bill_eur'])

    if len(df_sb_raw) > 0:
        df_sb_daily = df_sb_raw.groupby('gas_day', as_index=False).agg(
            sb_qty_kwh=('qty_kwh', 'sum'), sb_up=('up_eur_kwh', 'first'), sb_eur=('amount_eur', 'sum'))
    else:
        df_sb_daily = pd.DataFrame(columns=['gas_day', 'sb_qty_kwh', 'sb_up', 'sb_eur'])

    print(f"{'='*70}")
    print(f"ALLOCATION SETTLEMENT — {SETTLE_MONTH_LABEL}")
    print(f"{'='*70}")
    if len(df_bill_daily) > 0:
        print(f"\n--- PURCHASES (Bill) — {len(df_bill_daily)} day(s) ---")
        display(df_bill_daily)
        print(f"Total: {df_bill_daily['bill_qty_kwh'].sum():,.0f} kWh | EUR {df_bill_daily['bill_eur'].sum():,.2f}")
    else:
        print("\n\u26a0\ufe0f No purchase (bill) entries found")
    if len(df_sb_daily) > 0:
        print(f"\n--- SALES (Self-Bill) — {len(df_sb_daily)} day(s) ---")
        display(df_sb_daily)
        print(f"Total: {df_sb_daily['sb_qty_kwh'].sum():,.0f} kWh | EUR {df_sb_daily['sb_eur'].sum():,.2f}")
    else:
        print("\n\u26a0\ufe0f No sales (self-bill) entries found")

    # --- Dispatch deltas ---
    df_disp_raw = (
        spark.table(DISPATCH_TABLE)
        .filter(
            (F.col("country") == COUNTRY) & (F.col("balancing_country") == COUNTRY) &
            (F.col("gas_day").between(str(SETTLE_START), str(SETTLE_END))) &
            (F.col("location_id").isin(END_USER_LOCATIONS))
        )
        .groupBy("gas_day", "location_id")
        .agg(F.sum("preliminary_allocation").alias("prelim_kwh"),
             F.sum("final_allocation").alias("final_kwh"))
        .toPandas()
    )
    df_disp_raw['gas_day'] = pd.to_datetime(df_disp_raw['gas_day']).dt.date
    has_final = (df_disp_raw['final_kwh'].notna().any()
                 and (df_disp_raw['final_kwh'].fillna(0) != 0).any())
    if not has_final:
        print("\n\u26a0\ufe0f final_allocation is NOT populated for end users in dispatch.")

    df_disp_raw['delta_kwh'] = df_disp_raw['final_kwh'].fillna(0) - df_disp_raw['prelim_kwh'].fillna(0)
    df_disp_raw.loc[df_disp_raw['delta_kwh'].abs() < 10, 'delta_kwh'] = 0

    purch = (df_disp_raw[df_disp_raw['delta_kwh'] < 0]
             .groupby('gas_day', as_index=False)
             .agg(eq_purchase_kwh=('delta_kwh', lambda x: abs(x.sum()))))
    sale  = (df_disp_raw[df_disp_raw['delta_kwh'] > 0]
             .groupby('gas_day', as_index=False)
             .agg(eq_sale_kwh=('delta_kwh', 'sum')))

    all_days = pd.DataFrame({'gas_day': pd.date_range(SETTLE_START, SETTLE_END).date})
    dispatch_daily = all_days.merge(purch, on='gas_day', how='left').merge(sale, on='gas_day', how='left').fillna(0)

    # --- ZTP prices ---
    pub_start = SETTLE_START - timedelta(days=1)
    pub_end   = SETTLE_END   - timedelta(days=1)
    df_prices = spark.sql(f"""
        SELECT DATE_ADD(publication_date, 1) AS gas_day,
               publisher_column_value / 1000.0 AS ztp_eur_kwh
        FROM {PRICE_TABLE}
        WHERE keys = '{PRICE_KEYS}' AND publisher_column_name = '{PRICE_COLUMN}'
          AND data_package = '{PRICE_DATA_PACKAGE}'
          AND publication_date >= '{pub_start}' AND publication_date <= '{pub_end}'
    """).toPandas()
    df_prices['gas_day'] = pd.to_datetime(df_prices['gas_day']).dt.date
    print(f"\nZTP prices loaded: {len(df_prices)} days")

# COMMAND ----------

# DBTITLE 1,Allocation Settlement — Compare & Flag
# =============================================================================
# ALLOCATION SETTLEMENT — COMPARISON GRID & FLAGS
# =============================================================================

if not HAS_ALLOC:
    print("Allocation settlement skipped.")
else:
    alloc_comp = (
        all_days
        .merge(df_bill_daily, on='gas_day', how='left')
        .merge(df_sb_daily,   on='gas_day', how='left')
        .merge(df_prices,     on='gas_day', how='left')
        .merge(dispatch_daily, on='gas_day', how='left')
        .fillna(0)
    )

    # Price verification
    alloc_comp['bill_price_diff'] = None
    alloc_comp.loc[alloc_comp['bill_qty_kwh'] > 0, 'bill_price_diff'] = (
        alloc_comp.loc[alloc_comp['bill_qty_kwh'] > 0, 'bill_up']
      - alloc_comp.loc[alloc_comp['bill_qty_kwh'] > 0, 'ztp_eur_kwh']).round(6)
    alloc_comp['sb_price_diff'] = None
    alloc_comp.loc[alloc_comp['sb_qty_kwh'] > 0, 'sb_price_diff'] = (
        alloc_comp.loc[alloc_comp['sb_qty_kwh'] > 0, 'sb_up']
      - alloc_comp.loc[alloc_comp['sb_qty_kwh'] > 0, 'ztp_eur_kwh']).round(6)

    alloc_comp['eq_purchase_eur'] = alloc_comp['eq_purchase_kwh'] * alloc_comp['ztp_eur_kwh']
    alloc_comp['eq_sale_eur']     = alloc_comp['eq_sale_kwh']     * alloc_comp['ztp_eur_kwh']
    alloc_comp['qty_diff_kwh'] = (alloc_comp['eq_sale_kwh'] - alloc_comp['eq_purchase_kwh']) \
                               - (alloc_comp['sb_qty_kwh']  - alloc_comp['bill_qty_kwh'])
    alloc_comp['eur_diff']     = (alloc_comp['eq_sale_eur'] - alloc_comp['eq_purchase_eur']) \
                               - (alloc_comp['sb_eur']      - alloc_comp['bill_eur'])

    # Flag logic
    FLAG_KWH_THRESHOLD = 10
    FLAG_EUR_THRESHOLD = 1.0
    alloc_comp['flag'] = ''
    bpd = pd.to_numeric(alloc_comp['bill_price_diff'], errors='coerce')
    spd = pd.to_numeric(alloc_comp['sb_price_diff'], errors='coerce')
    alloc_comp.loc[bpd.notna() & (bpd.abs() > 0.0001), 'flag'] = 'Price mismatch (bill)'
    alloc_comp.loc[spd.notna() & (spd.abs() > 0.0001), 'flag'] = 'Price mismatch (self-bill)'
    if has_final:
        alloc_comp.loc[(alloc_comp['bill_qty_kwh'] > 0) & (alloc_comp['eq_purchase_kwh'] == 0), 'flag'] = 'Bill entry, no dispatch purchase'
        alloc_comp.loc[(alloc_comp['eq_purchase_kwh'] > FLAG_KWH_THRESHOLD) & (alloc_comp['bill_qty_kwh'] == 0), 'flag'] = 'Dispatch purchase, no bill entry'
        alloc_comp.loc[(alloc_comp['sb_qty_kwh'] > 0) & (alloc_comp['eq_sale_kwh'] == 0), 'flag'] = 'Self-bill entry, no dispatch sale'
        alloc_comp.loc[(alloc_comp['eq_sale_kwh'] > FLAG_KWH_THRESHOLD) & (alloc_comp['sb_qty_kwh'] == 0), 'flag'] = 'Dispatch sale, no self-bill entry'
        alloc_comp.loc[(alloc_comp['flag'] == '') & (alloc_comp['qty_diff_kwh'].abs() > FLAG_KWH_THRESHOLD), 'flag'] = 'Qty mismatch'
        alloc_comp.loc[(alloc_comp['flag'] == '') & (alloc_comp['eur_diff'].abs() > FLAG_EUR_THRESHOLD), 'flag'] = 'EUR mismatch'

    # Monthly summary
    inv_purch_kwh = alloc_comp['bill_qty_kwh'].sum()
    inv_purch_eur = alloc_comp['bill_eur'].sum()
    inv_sale_kwh  = alloc_comp['sb_qty_kwh'].sum()
    inv_sale_eur  = alloc_comp['sb_eur'].sum()
    eq_purch_kwh  = alloc_comp['eq_purchase_kwh'].sum()
    eq_purch_eur  = alloc_comp['eq_purchase_eur'].sum()
    eq_sale_kwh   = alloc_comp['eq_sale_kwh'].sum()
    eq_sale_eur   = alloc_comp['eq_sale_eur'].sum()

    all_price_ok = True
    if alloc_comp[alloc_comp['bill_qty_kwh'] > 0]['bill_price_diff'].abs().max() > 0.0001:
        all_price_ok = False
    if alloc_comp[alloc_comp['sb_qty_kwh'] > 0]['sb_price_diff'].abs().max() > 0.0001:
        all_price_ok = False

    alloc_flagged = alloc_comp[alloc_comp['flag'] != ''].copy()
    na = 'N/A'
    def _flag(line, diff_eur):
        if not has_final: return '\u23f3 Awaiting final alloc'
        if abs(diff_eur) < FLAG_EUR_THRESHOLD: return '\u2705'
        return f'\u26a0\ufe0f \u0394 {diff_eur:+,.2f} EUR'

    purch_diff = eq_purch_eur - inv_purch_eur if has_final else 0
    sale_diff  = eq_sale_eur - inv_sale_eur if has_final else 0
    net_diff   = (eq_sale_eur - eq_purch_eur) - (inv_sale_eur - inv_purch_eur) if has_final else 0
    invoice_dt = pd.Timestamp(SETTLE_START) + pd.DateOffset(months=3)
    ALLOC_INVOICE_MONTH = invoice_dt.strftime('%B %Y')

    alloc_summary_rows = [
        {'Invoice Month': ALLOC_INVOICE_MONTH, 'Line': 'Purchases (bill)',
         'Invoice kWh': f"{inv_purch_kwh:,.0f}", 'Invoice EUR': f"{inv_purch_eur:,.2f}",
         'Equinor kWh': f"{eq_purch_kwh:,.0f}" if has_final else na,
         'Equinor EUR': f"{eq_purch_eur:,.2f}" if has_final else na,
         'Diff EUR': f"{purch_diff:+,.2f}" if has_final else na, 'Status': _flag('purch', purch_diff)},
        {'Invoice Month': ALLOC_INVOICE_MONTH, 'Line': 'Sales (self-bill)',
         'Invoice kWh': f"{inv_sale_kwh:,.0f}", 'Invoice EUR': f"{inv_sale_eur:,.2f}",
         'Equinor kWh': f"{eq_sale_kwh:,.0f}" if has_final else na,
         'Equinor EUR': f"{eq_sale_eur:,.2f}" if has_final else na,
         'Diff EUR': f"{sale_diff:+,.2f}" if has_final else na, 'Status': _flag('sale', sale_diff)},
        {'Invoice Month': ALLOC_INVOICE_MONTH, 'Line': 'NET (sales \u2212 purchases)',
         'Invoice kWh': f"{inv_sale_kwh - inv_purch_kwh:+,.0f}",
         'Invoice EUR': f"{inv_sale_eur - inv_purch_eur:+,.2f}",
         'Equinor kWh': f"{eq_sale_kwh - eq_purch_kwh:+,.0f}" if has_final else na,
         'Equinor EUR': f"{eq_sale_eur - eq_purch_eur:+,.2f}" if has_final else na,
         'Diff EUR': f"{net_diff:+,.2f}" if has_final else na, 'Status': _flag('net', net_diff)},
        {'Invoice Month': ALLOC_INVOICE_MONTH, 'Line': 'ZTP Price Check',
         'Invoice kWh': '', 'Invoice EUR': '', 'Equinor kWh': '', 'Equinor EUR': '',
         'Diff EUR': '', 'Status': '\u2705 All match' if all_price_ok else '\u26a0\ufe0f Mismatch found'},
    ]
    print(f"\n{'='*70}")
    print(f"MONTHLY SUMMARY \u2014 {SETTLE_MONTH_LABEL}")
    print(f"{'='*70}")
    display(pd.DataFrame(alloc_summary_rows))
    if len(alloc_flagged) > 0:
        print(f"\n\u26a0\ufe0f {len(alloc_flagged)} flagged day(s):")
        display(alloc_flagged)
    else:
        print(f"\n\u2705 No flagged days \u2014 all daily entries match within tolerance.")

# COMMAND ----------

# DBTITLE 1,Allocation Settlement — Save
# =============================================================================
# ALLOCATION SETTLEMENT — SAVE TO DELTA
# =============================================================================
if not HAS_ALLOC:
    print("Allocation settlement save skipped.")
else:
    run_ts = datetime.now()
    print(f"Saving allocation settlement \u2014 {SETTLE_MONTH_LABEL} ({SETTLE_CK}):")
    _d = alloc_comp.copy()
    _d['bill_price_diff'] = pd.to_numeric(_d['bill_price_diff'], errors='coerce')
    _d['sb_price_diff'] = pd.to_numeric(_d['sb_price_diff'], errors='coerce')
    _d['check_month'] = SETTLE_CK
    _d['run_timestamp'] = run_ts
    _save_delta(_d, ALLOC_DAILY_TABLE, SETTLE_CK)
    _s = pd.DataFrame([
        {'line': 'Purchases', 'invoice_kwh': float(inv_purch_kwh), 'invoice_eur': float(inv_purch_eur),
         'equinor_kwh': float(eq_purch_kwh) if has_final else None,
         'equinor_eur': float(eq_purch_eur) if has_final else None,
         'diff_eur': float(purch_diff) if has_final else None, 'status': _flag('purch', purch_diff)},
        {'line': 'Sales', 'invoice_kwh': float(inv_sale_kwh), 'invoice_eur': float(inv_sale_eur),
         'equinor_kwh': float(eq_sale_kwh) if has_final else None,
         'equinor_eur': float(eq_sale_eur) if has_final else None,
         'diff_eur': float(sale_diff) if has_final else None, 'status': _flag('sale', sale_diff)},
        {'line': 'Net', 'invoice_kwh': float(inv_sale_kwh - inv_purch_kwh),
         'invoice_eur': float(inv_sale_eur - inv_purch_eur),
         'equinor_kwh': float(eq_sale_kwh - eq_purch_kwh) if has_final else None,
         'equinor_eur': float(eq_sale_eur - eq_purch_eur) if has_final else None,
         'diff_eur': float(net_diff) if has_final else None, 'status': _flag('net', net_diff)},
        {'line': 'ZTP Price Check', 'invoice_kwh': None, 'invoice_eur': None,
         'equinor_kwh': None, 'equinor_eur': None, 'diff_eur': None,
         'status': '\u2705 All match' if all_price_ok else '\u26a0\ufe0f Mismatch found'},
    ])
    _s['invoice_month'] = ALLOC_INVOICE_MONTH
    _s['check_month'] = SETTLE_CK
    _s['run_timestamp'] = run_ts
    _save_delta(_s, ALLOC_SUMMARY_TABLE, SETTLE_CK)
    _f = alloc_flagged.copy()
    if len(_f) > 0:
        _f['bill_price_diff'] = pd.to_numeric(_f['bill_price_diff'], errors='coerce')
        _f['sb_price_diff'] = pd.to_numeric(_f['sb_price_diff'], errors='coerce')
        _f['check_month'] = SETTLE_CK
        _f['run_timestamp'] = run_ts
    _save_delta(_f, ALLOC_FLAGS_TABLE, SETTLE_CK)
    print(f"Allocation settlement save complete.")

# COMMAND ----------

# DBTITLE 1,Allocation Settlement — Save
# =============================================================================
# ALLOCATION SETTLEMENT — SAVE TO DELTA
# =============================================================================

if not HAS_ALLOC:
    print("Allocation settlement save skipped.")
else:
    run_ts = datetime.now()
    print(f"Saving allocation settlement \u2014 {SETTLE_MONTH_LABEL} ({SETTLE_CK}):")

    # --- Daily grid ---
    _d = alloc_comp.copy()
    _d['bill_price_diff'] = pd.to_numeric(_d['bill_price_diff'], errors='coerce')
    _d['sb_price_diff'] = pd.to_numeric(_d['sb_price_diff'], errors='coerce')
    _d['check_month'] = SETTLE_CK
    _d['run_timestamp'] = run_ts
    _save_delta(_d, ALLOC_DAILY_TABLE, SETTLE_CK)

    # --- Monthly summary ---
    _s = pd.DataFrame([
        {'line': 'Purchases', 'invoice_kwh': float(inv_purch_kwh), 'invoice_eur': float(inv_purch_eur),
         'equinor_kwh': float(eq_purch_kwh) if has_final else None,
         'equinor_eur': float(eq_purch_eur) if has_final else None,
         'diff_eur': float(purch_diff) if has_final else None, 'status': _flag('purch', purch_diff)},
        {'line': 'Sales', 'invoice_kwh': float(inv_sale_kwh), 'invoice_eur': float(inv_sale_eur),
         'equinor_kwh': float(eq_sale_kwh) if has_final else None,
         'equinor_eur': float(eq_sale_eur) if has_final else None,
         'diff_eur': float(sale_diff) if has_final else None, 'status': _flag('sale', sale_diff)},
        {'line': 'Net', 'invoice_kwh': float(inv_sale_kwh - inv_purch_kwh),
         'invoice_eur': float(inv_sale_eur - inv_purch_eur),
         'equinor_kwh': float(eq_sale_kwh - eq_purch_kwh) if has_final else None,
         'equinor_eur': float(eq_sale_eur - eq_purch_eur) if has_final else None,
         'diff_eur': float(net_diff) if has_final else None, 'status': _flag('net', net_diff)},
        {'line': 'ZTP Price Check', 'invoice_kwh': None, 'invoice_eur': None,
         'equinor_kwh': None, 'equinor_eur': None, 'diff_eur': None,
         'status': '\u2705 All match' if all_price_ok else '\u26a0\ufe0f Mismatch found'},
    ])
    _s['invoice_month'] = ALLOC_INVOICE_MONTH
    _s['check_month'] = SETTLE_CK
    _s['run_timestamp'] = run_ts
    _save_delta(_s, ALLOC_SUMMARY_TABLE, SETTLE_CK)

    # --- Flagged entries ---
    _f = alloc_flagged.copy()
    if len(_f) > 0:
        _f['bill_price_diff'] = pd.to_numeric(_f['bill_price_diff'], errors='coerce')
        _f['sb_price_diff'] = pd.to_numeric(_f['sb_price_diff'], errors='coerce')
        _f['check_month'] = SETTLE_CK
        _f['run_timestamp'] = run_ts
    _save_delta(_f, ALLOC_FLAGS_TABLE, SETTLE_CK)
    print(f"\nAllocation settlement save complete.")

# COMMAND ----------

# DBTITLE 1,Variable Fee — Parse Invoice
# =============================================================================
# VARIABLE TRADING FEE — PARSE INVOICE
# =============================================================================

if len(vf) == 0:
    print(f"\u26a0\ufe0f No Variable Fee (ZTP Trading) entries in this invoice for {CHECK_MONTH}")
    vf_daily = pd.DataFrame()
    vf_monthly = pd.DataFrame()
else:
    entry = vf[vf['direction'] == 'Entry'].groupby('gas_day').agg(
        entry_qty_mwh=('qty', 'sum'), up_eur_mwh=('up', 'first'),
        entry_amount_eur=('amount', 'sum')
    ).reset_index()
    exit_ = vf[vf['direction'] == 'Exit'].groupby('gas_day').agg(
        exit_qty_mwh=('qty', 'sum'), exit_amount_eur=('amount', 'sum')
    ).reset_index()

    vf_daily = entry.merge(exit_, on='gas_day', how='outer').sort_values('gas_day').reset_index(drop=True)
    vf_daily['total_amount_eur'] = vf_daily['entry_amount_eur'] + vf_daily['exit_amount_eur']

    print(f"\n{'='*70}")
    print(f"VARIABLE FEE (ZTP Trading) \u2014 DAILY ({CHECK_MONTH})")
    print(f"{'='*70}")
    display(vf_daily)

    vf_monthly = pd.DataFrame([{
        'billing_month': CHECK_MONTH,
        'rate_eur_mwh': vf_daily['up_eur_mwh'].iloc[0],
        'entry_qty_mwh': vf_daily['entry_qty_mwh'].sum(),
        'exit_qty_mwh': vf_daily['exit_qty_mwh'].sum(),
        'entry_amount_eur': vf_daily['entry_amount_eur'].sum(),
        'exit_amount_eur': vf_daily['exit_amount_eur'].sum(),
        'total_amount_eur': vf_daily['total_amount_eur'].sum(),
    }])
    print(f"\nMONTHLY SUMMARY:")
    display(vf_monthly)

# COMMAND ----------

# DBTITLE 1,Variable Fee — Equinor ZTPH Volumes
# =============================================================================
# VARIABLE FEE — EQUINOR ZTPH VOLUMES (dispatch + Endurast AST)
# =============================================================================

def get_ztph_shipper_daily(m_start, m_end, mapping_path):
    """Pull ZTPH volumes using NAIVE DATE strategy."""
    full_map = pd.read_excel(mapping_path)
    full_map['VALID_FROM'] = pd.to_datetime(full_map['VALID_FROM'])
    full_map['VALID_TO']   = pd.to_datetime(full_map['VALID_TO'])
    ztph_map = full_map[full_map['NODE_ID_DELIVERY'] == 'ZTPH'].copy()
    print(f"Mapping: {len(ztph_map)} ZTPH stems, {ztph_map['SHIPPER_CODE'].nunique()} unique shippers")

    df_raw = spark.table(DISPATCH_TABLE).filter(
        (F.col('country') == COUNTRY) & (F.col('balancing_country') == COUNTRY) &
        (F.col('location_id') == 'ZTPH') &
        (~F.lower(F.col('contract_type')).contains('optimize')) &
        (~F.lower(F.col('contract_type')).contains('balance')) &
        (F.col('gas_day') >= str(m_start)) & (F.col('gas_day') <= str(m_end)) &
        F.col('nomination').isNotNull()
    )
    agg_pd = (
        df_raw.groupBy('gas_day', 'contract_id', 'contract_type', 'quantity_unit')
        .agg(F.sum('nomination').alias('volume_kwh')).toPandas()
    )
    agg_pd['gas_day']    = pd.to_datetime(agg_pd['gas_day']).dt.date
    agg_pd['gas_day_ts'] = pd.to_datetime(agg_pd['gas_day'])
    print(f"Dispatch: {len(agg_pd)} contract-day rows")
    if len(agg_pd) == 0:
        return pd.DataFrame(), agg_pd

    merged = agg_pd.merge(
        ztph_map[['CONTRACT_GROUP_ID', 'SHIPPER_CODE', 'VALID_FROM', 'VALID_TO']],
        left_on='contract_id', right_on='CONTRACT_GROUP_ID', how='left')
    has_mapping = merged['CONTRACT_GROUP_ID'].notna()
    date_valid = (
        (merged['VALID_FROM'].isna() | (merged['VALID_FROM'] <= merged['gas_day_ts'])) &
        (merged['VALID_TO'].isna()   | (merged['VALID_TO']   >= merged['gas_day_ts'])))
    merged = merged[~has_mapping | date_valid].copy()
    unmapped = merged[merged['SHIPPER_CODE'].isna()]['contract_id'].unique().tolist()
    if unmapped: print(f"\u26a0\ufe0f Unmapped contracts: {unmapped}")

    mapped = merged[merged['SHIPPER_CODE'].notna()].copy()
    mapped['signed_kwh'] = mapped['volume_kwh']
    disp_daily = mapped.groupby(['gas_day', 'SHIPPER_CODE']).agg(
        signed_kwh=('signed_kwh', 'sum')).reset_index().rename(columns={'SHIPPER_CODE': 'ShipperCode'})

    # Endurast AST
    ENDURAST_HOURLY_TABLE = "ms_atlas.endurast_raw.hourly_v1latest"
    w = Window.partitionBy('counterparty', 'shippercode', 'time').orderBy(F.col('enqueued_time').desc())
    df_endu_raw = (
        spark.table(ENDURAST_HOURLY_TABLE)
        .filter((F.col('location') == 'ZTPH Hub (H-Zone)') &
                (F.to_date('time') >= str(m_start)) & (F.to_date('time') <= str(m_end)))
        .withColumn('rn', F.row_number().over(w)).filter(F.col('rn') == 1))
    endu_pre_agg = (
        df_endu_raw.withColumn('gas_day', F.to_date('time'))
        .select('gas_day', 'counterparty', F.col('shippercode').alias('ShipperCode'), 'unit', 'quantity').toPandas())
    endu_pre_agg['gas_day'] = pd.to_datetime(endu_pre_agg['gas_day']).dt.date
    agg_endu = endu_pre_agg.groupby(['gas_day', 'ShipperCode', 'unit'], as_index=False).agg(
        signed_kwh=('quantity', 'sum'))
    print(f"Endurast AST: {len(agg_endu)} shipper-day rows")

    combined = pd.concat([
        disp_daily[['gas_day', 'ShipperCode', 'signed_kwh']],
        agg_endu[['gas_day',  'ShipperCode', 'signed_kwh']],
    ], ignore_index=True)
    shipper_daily = combined.groupby(['gas_day', 'ShipperCode']).agg(
        net_kwh=('signed_kwh', 'sum')).reset_index()
    shipper_daily['position'] = shipper_daily['net_kwh'].apply(
        lambda x: 'NET BUY' if x > 0 else ('NET SELL' if x < 0 else 'FLAT'))
    return shipper_daily, merged, agg_endu, endu_pre_agg

if len(vf_daily) > 0:
    vf_shipper_daily, vf_contract_detail, vf_endurast_detail, vf_endurast_txn = get_ztph_shipper_daily(
        MONTH_START, MONTH_END, MAPPING_PATH)
    if len(vf_shipper_daily) > 0:
        print(f"\n{'='*75}")
        print(f"ZTPH COMBINED NET POSITIONS \u2014 DAILY ({CHECK_MONTH})")
        print(f"{'='*75}")
        display(vf_shipper_daily.sort_values(['gas_day', 'ShipperCode']))
    else:
        print(f"\n\u26a0\ufe0f No ZTPH positions found for {CHECK_MONTH}.")
else:
    vf_shipper_daily = pd.DataFrame()
    print("Variable Fee skipped (no invoice entries).")

# COMMAND ----------

# DBTITLE 1,Variable Fee — Compare & Save
# =============================================================================
# VARIABLE TRADING FEE — COMPARE & SAVE
# =============================================================================

if len(vf_daily) == 0 or len(vf_shipper_daily) == 0:
    print("Variable Trading Fee comparison skipped (no data).")
    vf_verdict = 'SKIPPED'
    vf_num_flagged = 0
else:
    daily_exit = (
        vf_shipper_daily[vf_shipper_daily['net_kwh'] < 0]
        .groupby('gas_day', as_index=False)['net_kwh'].sum()
        .assign(eq_exit_kwh=lambda x: x['net_kwh'].abs())
        .drop(columns='net_kwh'))
    up_eur_mwh = vf_daily['up_eur_mwh'].iloc[0]

    vf_all_days = pd.DataFrame({'gas_day': pd.date_range(MONTH_START, MONTH_END).date})
    vf_comp = (
        vf_all_days
        .merge(vf_daily[['gas_day', 'entry_qty_mwh', 'exit_qty_mwh',
                          'entry_amount_eur', 'exit_amount_eur', 'total_amount_eur']],
               on='gas_day', how='left')
        .merge(daily_exit, on='gas_day', how='left')
        .fillna(0))
    safe_div = lambda a, b: (a / b.replace(0, float('nan')) * 100).round(2)

    vf_comp['eq_exit_mwh']      = vf_comp['eq_exit_kwh'] / 1000.0
    vf_comp['exit_diff_mwh']    = vf_comp['eq_exit_mwh'] - vf_comp['exit_qty_mwh']
    vf_comp['exit_diff_pct']    = safe_div(vf_comp['exit_diff_mwh'], vf_comp['exit_qty_mwh'])
    vf_comp['eq_exit_cost_eur'] = vf_comp['eq_exit_mwh'] * up_eur_mwh
    vf_comp['cost_diff_eur']    = vf_comp['eq_exit_cost_eur'] - vf_comp['exit_amount_eur']
    vf_comp['flag'] = ''
    mask_exit = vf_comp['exit_diff_pct'].abs() > 1.0
    vf_comp.loc[mask_exit, 'flag'] = '\u26a0\ufe0f EXIT'

    vf_flagged = vf_comp[vf_comp['flag'] != '']
    vf_num_flagged = len(vf_flagged)
    status = '\u2705 ALL MATCH' if vf_num_flagged == 0 else f'\u26a0\ufe0f {vf_num_flagged} DAY(S) FLAGGED'

    print(f"\n{'='*90}")
    print(f"VARIABLE TRADING FEE \u2014 DAILY COMPARISON ({CHECK_MONTH})  [{status}]")
    print(f"{'='*90}")
    display(vf_comp[['gas_day', 'exit_qty_mwh', 'eq_exit_mwh', 'exit_diff_pct',
                     'exit_amount_eur', 'eq_exit_cost_eur', 'cost_diff_eur', 'flag']])

    inv_exit_total    = vf_comp['exit_qty_mwh'].sum()
    eq_exit_total     = vf_comp['eq_exit_mwh'].sum()
    exit_vol_diff_pct = (eq_exit_total - inv_exit_total) / inv_exit_total * 100 if inv_exit_total else 0
    inv_exit_eur      = vf_comp['exit_amount_eur'].sum()
    eq_exit_eur       = vf_comp['eq_exit_cost_eur'].sum()
    exit_cost_diff    = eq_exit_eur - inv_exit_eur
    vf_verdict        = 'VERIFIED' if abs(exit_vol_diff_pct) < 1.0 else 'REVIEW REQUIRED'

    entry_exit_match = (abs(vf_comp['entry_qty_mwh'] - vf_comp['exit_qty_mwh']) < 0.01).all()
    eq_grand_total   = eq_exit_eur * 2
    inv_grand_total  = vf_comp['total_amount_eur'].sum()
    grand_total_diff = eq_grand_total - inv_grand_total

    print(f"\n{'='*90}")
    print(f"MONTHLY SUMMARY \u2014 Variable Trading Fee ({CHECK_MONTH})")
    print(f"{'='*90}")
    print(f"  Verdict: {'\u2705' if vf_verdict == 'VERIFIED' else '\u26a0\ufe0f'} {vf_verdict}")
    print(f"  Exit vol diff: {exit_vol_diff_pct:+.4f}%  |  Cost diff: EUR {exit_cost_diff:+,.2f}")
    print(f"  Double-charge: {'\u2705 CONFIRMED' if entry_exit_match else '\u26a0\ufe0f NOT UNIFORM'}")

    # --- SAVE ---
    run_ts = datetime.now()
    print(f"\nSaving Variable Fee results:")
    vf_res = pd.DataFrame([{
        'check_month': CHECK_MONTH, 'category': 'ZTP Trading', 'line_type': 'Variable Trading Fee',
        'invoice_file': INVOICE_FILE, 'invoice_qty': float(inv_exit_total),
        'equinor_qty': float(eq_exit_total), 'qty_diff_pct': float(exit_vol_diff_pct),
        'invoice_eur': float(inv_grand_total), 'equinor_eur': float(eq_grand_total),
        'eur_diff': float(grand_total_diff), 'days_flagged': vf_num_flagged,
        'price_match': True, 'double_charge_confirmed': bool(entry_exit_match),
        'verdict': vf_verdict, 'run_timestamp': run_ts,
    }])
    _save_delta(vf_res, VF_RESULTS_TABLE, CHECK_MONTH)

    if vf_num_flagged > 0:
        flag_rows = [{'check_month': CHECK_MONTH, 'gas_day': str(row['gas_day']),
            'category': 'ZTP Trading', 'line_type': 'Variable Trading Fee',
            'inv_qty': float(row['exit_qty_mwh']), 'eq_qty': float(row['eq_exit_mwh']),
            'qty_diff': float(row['exit_diff_mwh']), 'qty_diff_pct': float(row['exit_diff_pct']),
            'inv_up': float(up_eur_mwh), 'eq_price': float(up_eur_mwh),
            'price_match': True, 'flag': row['flag'].strip(), 'run_timestamp': run_ts,
        } for _, row in vf_flagged.iterrows()]
        vf_flags_df = pd.DataFrame(flag_rows)
    else:
        vf_flags_df = pd.DataFrame()
    _save_delta(vf_flags_df, VF_FLAGS_TABLE, CHECK_MONTH)
    print(f"Variable Fee save complete.")

# COMMAND ----------

# DBTITLE 1,Energy in Cash — Equinor Pull & Compare
# =============================================================================
# ENERGY IN CASH — EQUINOR PULL & COMPARE
# Strategy copied from Variable Fee Checker (Belgium - Fluxys):
#   1. Domestic volumes (TESSENDERLO, ANTWERP JUPITER) from dispatch
#   2. IC volumes (Flow contracts, excl hub internal) — sign = direction
#   3. ZTP spot prices (EEX EGSI, publication_date + 1 day)
#   4. Convert volumes via QTY_MULTIPLIER (0.0008)
#   5. Compare invoice vs Equinor per category per day → monthly summary
# =============================================================================

if len(eic) == 0:
    print("Energy in Cash comparison skipped — no entries in this invoice.")
    eic_comparisons = {}
    eic_summary = pd.DataFrame()
else:
    # --- Pull Equinor dispatch volumes ---
    df_dom = spark.table(DISPATCH_TABLE).filter(
        (F.col("country") == COUNTRY) & (F.col("balancing_country") == COUNTRY) &
        (F.col("gas_day") >= str(MONTH_START)) & (F.col("gas_day") <= str(MONTH_END)) &
        (F.col("location_id").isin(DOMESTIC_LOCATIONS))
    )
    domestic_daily = df_dom.groupBy("gas_day").agg(
        F.sum("preliminary_allocation").alias("domestic_kwh")
    )

    df_ic = spark.table(DISPATCH_TABLE).filter(
        (F.col("country") == COUNTRY) & (F.col("balancing_country") == COUNTRY) &
        (F.col("gas_day") >= str(MONTH_START)) & (F.col("gas_day") <= str(MONTH_END)) &
        (F.col("contract_type") == "Flow") &
        (~F.col("location_id").isin(DOMESTIC_LOCATIONS + HUB_INTERNAL_LOCATIONS))
    )
    ic_by_loc = df_ic.groupBy("gas_day", "location_id").agg(
        F.sum("preliminary_allocation").alias("loc_kwh")
    )
    ic_entry = ic_by_loc.filter(F.col("loc_kwh") > 0).groupBy("gas_day").agg(
        F.sum("loc_kwh").alias("entry_ic_kwh")
    )
    ic_exit = ic_by_loc.filter(F.col("loc_kwh") < 0).groupBy("gas_day").agg(
        F.sum("loc_kwh").alias("exit_ic_kwh")
    )

    # --- Pull ZTP spot prices ---
    pub_start = MONTH_START - timedelta(days=1)
    pub_end = MONTH_END - timedelta(days=1)
    df_prices = spark.sql(f"""
        SELECT DATE_ADD(publication_date, 1) AS gas_day,
               publisher_column_value / 1000.0 AS price_eur_kwh
        FROM {PRICE_TABLE}
        WHERE keys = '{PRICE_KEYS}' AND publisher_column_name = '{PRICE_COLUMN}'
          AND data_package = '{PRICE_DATA_PACKAGE}'
          AND publication_date >= '{pub_start}' AND publication_date <= '{pub_end}'
    """)

    # --- Join into single DataFrame ---
    all_days = spark.sql(f"SELECT explode(sequence(DATE '{MONTH_START}', DATE '{MONTH_END}', INTERVAL 1 DAY)) AS gas_day")
    equinor_eic = (
        all_days
        .join(domestic_daily, "gas_day", "left")
        .join(ic_entry, "gas_day", "left")
        .join(ic_exit, "gas_day", "left")
        .join(df_prices, "gas_day", "left")
        .fillna(0, subset=["domestic_kwh", "entry_ic_kwh", "exit_ic_kwh"])
    )
    equinor_eic_pd = equinor_eic.toPandas()
    equinor_eic_pd['gas_day'] = pd.to_datetime(equinor_eic_pd['gas_day']).dt.date

    # Convert to QTY using multiplier
    equinor_eic_pd['entry_qty'] = equinor_eic_pd['entry_ic_kwh'].abs() * QTY_MULTIPLIER
    equinor_eic_pd['exit_ic_qty'] = equinor_eic_pd['exit_ic_kwh'].abs() * QTY_MULTIPLIER
    equinor_eic_pd['domestic_qty'] = equinor_eic_pd['domestic_kwh'].abs() * QTY_MULTIPLIER

    days_with_prices = len(equinor_eic_pd[equinor_eic_pd['price_eur_kwh'] > 0])
    print(f"Equinor EiC data: {len(equinor_eic_pd)} days, {days_with_prices} with prices")
    print(f"  Entry IC: {equinor_eic_pd['entry_ic_kwh'].sum():,.0f} kWh")
    print(f"  Exit IC:  {equinor_eic_pd['exit_ic_kwh'].sum():,.0f} kWh")
    print(f"  Domestic: {equinor_eic_pd['domestic_kwh'].sum():,.0f} kWh")

    # --- Compare invoice vs Equinor per category ---
    category_map = {
        'Entry at Interconnection Point': 'entry_qty',
        'Exit at Interconnection Point': 'exit_ic_qty',
        'Exit at Domestic Point': 'domestic_qty',
    }

    eic_comparisons = {}
    eic_summary_rows = []

    for cat, qty_col in category_map.items():
        inv_cat = eic[eic['category'] == cat].groupby('gas_day').agg(
            inv_qty=('qty', 'sum'),
            inv_up=('up', 'first'),
            inv_amount=('amount', 'sum')
        ).reset_index()

        if len(inv_cat) == 0:
            continue

        eq_cat = equinor_eic_pd[['gas_day', qty_col, 'price_eur_kwh']].copy()
        eq_cat.columns = ['gas_day', 'eq_qty', 'eq_price']

        comp = inv_cat.merge(eq_cat, on='gas_day', how='outer').sort_values('gas_day').reset_index(drop=True)
        comp['qty_diff'] = comp['eq_qty'] - comp['inv_qty']
        comp['qty_diff_pct'] = (comp['qty_diff'] / comp['inv_qty'] * 100).round(2)
        comp['price_match'] = (comp['inv_up'] - comp['eq_price']).abs() < 0.000001
        comp['eq_amount'] = comp['eq_qty'] * comp['eq_price']
        comp['amount_diff'] = comp['eq_amount'] - comp['inv_amount']

        import numpy as np
        has_exposure = ~((comp['eq_qty'].fillna(0).abs() < 0.0001) & (comp['inv_qty'].isna()))
        comp['flag'] = ''
        comp.loc[has_exposure & (comp['qty_diff_pct'].abs() > 1.0), 'flag'] = '\u26a0\ufe0f QTY'
        comp.loc[has_exposure & (~comp['price_match']), 'flag'] = (
            comp.loc[has_exposure & (~comp['price_match']), 'flag'] + ' \u26a0\ufe0f PRICE'
        )

        eic_comparisons[cat] = comp

        eic_summary_rows.append({
            'Category': cat,
            'Invoice QTY': inv_cat['inv_qty'].sum(),
            'Equinor QTY': comp['eq_qty'].sum(),
            'QTY Diff %': ((comp['eq_qty'].sum() - inv_cat['inv_qty'].sum()) / inv_cat['inv_qty'].sum() * 100),
            'Invoice EUR': inv_cat['inv_amount'].sum(),
            'Equinor EUR': comp['eq_amount'].sum(),
            'EUR Diff': comp['eq_amount'].sum() - inv_cat['inv_amount'].sum(),
            'Days Flagged': len(comp[comp['flag'] != '']),
            'Price Match': comp['price_match'].all()
        })

    eic_summary = pd.DataFrame(eic_summary_rows)

    # --- Display ---
    print(f"\n{'='*85}")
    print(f"ENERGY IN CASH \u2014 DAILY COMPARISON ({CHECK_MONTH})")
    print(f"{'='*85}")
    for cat, comp in eic_comparisons.items():
        flagged = comp[comp['flag'] != '']
        status = f"\u2705 ALL MATCH" if len(flagged) == 0 else f"\u26a0\ufe0f {len(flagged)} MISMATCH(ES)"
        print(f"\n--- {cat} [{status}] ---")
        display(comp[['gas_day', 'inv_qty', 'eq_qty', 'qty_diff', 'qty_diff_pct',
                      'inv_up', 'eq_price', 'price_match', 'flag']])

    if len(eic_summary) > 0:
        print(f"\n{'='*85}")
        print(f"MONTHLY SUMMARY \u2014 Energy in Cash ({CHECK_MONTH})")
        print(f"{'='*85}")
        display(eic_summary)

        total_inv = eic_summary['Invoice EUR'].sum()
        total_eq = eic_summary['Equinor EUR'].sum()
        total_diff = total_eq - total_inv
        all_prices_ok = eic_summary['Price Match'].all()
        total_flags = int(eic_summary['Days Flagged'].sum())

        print(f"\n  Total Invoice:  EUR {total_inv:,.2f}")
        print(f"  Total Equinor:  EUR {total_eq:,.2f}")
        print(f"  Difference:     EUR {total_diff:+,.2f} ({total_diff/total_inv*100:+.4f}%)")
        print(f"  Price Match:    {'\u2705 All correct' if all_prices_ok else '\u26a0\ufe0f Mismatches found'}")
        print(f"  Flagged Days:   {total_flags}")

        if abs(total_diff/total_inv*100) < 0.1 and all_prices_ok:
            print(f"\n  \u2705 INVOICE VERIFIED \u2014 within tolerance")
        else:
            print(f"\n  \u26a0\ufe0f REVIEW REQUIRED \u2014 see flagged days above")

# COMMAND ----------

# DBTITLE 1,Energy in Cash — Save
# =============================================================================
# ENERGY IN CASH — SAVE TO DELTA
# Uses eic_summary + eic_comparisons from previous cell
# Schema matches be_varfee_results for unified dashboard display
# =============================================================================

if len(eic) == 0:
    print("Energy in Cash save skipped — no entries in this invoice.")
else:
    run_ts = datetime.now()
    print(f"Saving Energy in Cash — {ARREARS_CHECK_MONTH}:")

    # --- Drop old table if schema changed (one-time migration) ---
    try:
        old_cols = [c.name for c in spark.table(EIC_RESULTS_TABLE).schema]
        if 'total_amount' in old_cols or 'equinor_eur' not in old_cols:
            spark.sql(f"DROP TABLE IF EXISTS {EIC_RESULTS_TABLE}")
            print(f"  \u267b\ufe0f Dropped old {EIC_RESULTS_TABLE} (schema migration)")
    except Exception:
        pass

    if len(eic_summary) > 0:
        # --- Save results with Equinor comparison ---
        result_rows = []
        for _, row in eic_summary.iterrows():
            inv_eur = float(row['Invoice EUR'])
            eq_eur = float(row['Equinor EUR'])
            eur_diff = float(row['EUR Diff'])
            result_rows.append({
                'check_month': ARREARS_CHECK_MONTH,
                'category': row['Category'],
                'line_type': 'Energy in Cash',
                'invoice_file': INVOICE_FILE,
                'invoice_qty': float(row['Invoice QTY']),
                'equinor_qty': float(row['Equinor QTY']),
                'qty_diff_pct': float(row['QTY Diff %']),
                'invoice_eur': inv_eur,
                'equinor_eur': eq_eur,
                'eur_diff': eur_diff,
                'days_flagged': int(row['Days Flagged']),
                'price_match': bool(row['Price Match']),
                'verdict': 'VERIFIED' if abs(eur_diff / inv_eur * 100) < 0.1 and bool(row['Price Match']) else 'REVIEW REQUIRED',
                'run_timestamp': run_ts,
            })
        eic_res = pd.DataFrame(result_rows)
    else:
        # Fallback: save raw invoice totals (no Equinor comparison available)
        eic_monthly_save = eic.groupby(['category'], as_index=False).agg(
            total_amount=('amount', 'sum'), total_qty=('qty', 'sum'),
        )
        eic_res = pd.DataFrame([{
            'check_month': ARREARS_CHECK_MONTH, 'category': r['category'],
            'line_type': 'Energy in Cash', 'invoice_file': INVOICE_FILE,
            'invoice_qty': float(r['total_qty']), 'equinor_qty': None,
            'qty_diff_pct': None, 'invoice_eur': float(r['total_amount']),
            'equinor_eur': None, 'eur_diff': None, 'days_flagged': None,
            'price_match': None, 'verdict': 'No comparison', 'run_timestamp': run_ts,
        } for _, r in eic_monthly_save.iterrows()])

    _save_delta(eic_res, EIC_RESULTS_TABLE, ARREARS_CHECK_MONTH)

    # --- Save flags (daily mismatches) ---
    flag_rows = []
    for cat, comp in eic_comparisons.items():
        flagged = comp[comp['flag'] != '']
        for _, row in flagged.iterrows():
            flag_rows.append({
                'check_month': ARREARS_CHECK_MONTH,
                'gas_day': str(row['gas_day']),
                'category': cat,
                'line_type': 'Energy in Cash',
                'inv_qty': float(row['inv_qty']),
                'eq_qty': float(row['eq_qty']),
                'qty_diff': float(row['qty_diff']),
                'qty_diff_pct': float(row['qty_diff_pct']),
                'inv_up': float(row['inv_up']),
                'eq_price': float(row['eq_price']),
                'price_match': bool(row['price_match']),
                'flag': row['flag'].strip(),
                'run_timestamp': run_ts,
            })

    eic_flags_df = pd.DataFrame(flag_rows) if flag_rows else pd.DataFrame()
    _save_delta(eic_flags_df, EIC_FLAGS_TABLE, ARREARS_CHECK_MONTH)

    # --- Display ---
    print(f"\nEnergy in Cash save complete.")
    display(eic_res[['category', 'invoice_eur', 'equinor_eur', 'eur_diff', 'verdict']])

# COMMAND ----------

# DBTITLE 1,Overall Summary
# =============================================================================
# OVERALL SUMMARY
# =============================================================================

print(f"{'='*80}")
print(f"INVOICE CHECKER BELGIUM v2 \u2014 OVERALL SUMMARY")
print(f"{'='*80}")
print(f"Invoice: {INVOICE_FILE}")
print(f"Invoice month: {invoice_month}")
print()

ltc_flag_count = len(ltc_flags)
if ltc_flag_count == 0:
    print(f"  \u2705 Long Term Capacity: PASS \u2014 all routes match")
else:
    print(f"  \u26a0\ufe0f Long Term Capacity: {ltc_flag_count} issue(s) flagged")

if not HAS_ALLOC:
    print(f"  \u23ed\ufe0f Allocation Settlement: SKIPPED (not in this invoice)")
else:
    alloc_flag_count = len(alloc_flagged) if isinstance(alloc_flagged, pd.DataFrame) and len(alloc_flagged) > 0 else 0
    if not has_final:
        print(f"  \u23f3 Allocation Settlement ({SETTLE_MONTH_LABEL}): Awaiting final allocations")
    elif alloc_flag_count == 0:
        print(f"  \u2705 Allocation Settlement ({SETTLE_MONTH_LABEL}): PASS")
    else:
        print(f"  \u26a0\ufe0f Allocation Settlement ({SETTLE_MONTH_LABEL}): {alloc_flag_count} day(s) flagged")

if len(vf) == 0:
    print(f"  \u23ed\ufe0f Variable Trading Fee: SKIPPED (not in this invoice)")
elif vf_verdict == 'SKIPPED':
    print(f"  \u23ed\ufe0f Variable Trading Fee: SKIPPED (no ZTPH data)")
elif vf_verdict == 'VERIFIED':
    print(f"  \u2705 Variable Trading Fee ({CHECK_MONTH}): PASS \u2014 {vf_verdict}")
else:
    print(f"  \u26a0\ufe0f Variable Trading Fee ({CHECK_MONTH}): {vf_verdict} \u2014 {vf_num_flagged} day(s) flagged")

print(f"\n{'='*80}")
print(f"All results saved to {NEW_SCHEMA}")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Allocation Settlement — Save
# =============================================================================
# ALLOCATION SETTLEMENT — SAVE TO DELTA
# =============================================================================

if not HAS_ALLOC:
    print("Allocation settlement save skipped.")
else:
    run_ts = datetime.now()
    print(f"Saving allocation settlement \u2014 {SETTLE_MONTH_LABEL} ({SETTLE_CK}):")

    # --- Daily grid ---
    _d = alloc_comp.copy()
    _d['bill_price_diff'] = pd.to_numeric(_d['bill_price_diff'], errors='coerce')
    _d['sb_price_diff'] = pd.to_numeric(_d['sb_price_diff'], errors='coerce')
    _d['check_month'] = SETTLE_CK
    _d['run_timestamp'] = run_ts
    _save_delta(_d, ALLOC_DAILY_TABLE, SETTLE_CK)

    # --- Monthly summary ---
    _s = pd.DataFrame([
        {'line': 'Purchases', 'invoice_kwh': float(inv_purch_kwh), 'invoice_eur': float(inv_purch_eur),
         'equinor_kwh': float(eq_purch_kwh) if has_final else None,
         'equinor_eur': float(eq_purch_eur) if has_final else None,
         'diff_eur': float(purch_diff) if has_final else None, 'status': _flag('purch', purch_diff)},
        {'line': 'Sales', 'invoice_kwh': float(inv_sale_kwh), 'invoice_eur': float(inv_sale_eur),
         'equinor_kwh': float(eq_sale_kwh) if has_final else None,
         'equinor_eur': float(eq_sale_eur) if has_final else None,
         'diff_eur': float(sale_diff) if has_final else None, 'status': _flag('sale', sale_diff)},
        {'line': 'Net', 'invoice_kwh': float(inv_sale_kwh - inv_purch_kwh),
         'invoice_eur': float(inv_sale_eur - inv_purch_eur),
         'equinor_kwh': float(eq_sale_kwh - eq_purch_kwh) if has_final else None,
         'equinor_eur': float(eq_sale_eur - eq_purch_eur) if has_final else None,
         'diff_eur': float(net_diff) if has_final else None, 'status': _flag('net', net_diff)},
        {'line': 'ZTP Price Check', 'invoice_kwh': None, 'invoice_eur': None,
         'equinor_kwh': None, 'equinor_eur': None, 'diff_eur': None,
         'status': '\u2705 All match' if all_price_ok else '\u26a0\ufe0f Mismatch found'},
    ])
    _s['invoice_month'] = ALLOC_INVOICE_MONTH
    _s['check_month'] = SETTLE_CK
    _s['run_timestamp'] = run_ts
    _save_delta(_s, ALLOC_SUMMARY_TABLE, SETTLE_CK)

    # --- Flagged entries ---
    _f = alloc_flagged.copy()
    if len(_f) > 0:
        _f['bill_price_diff'] = pd.to_numeric(_f['bill_price_diff'], errors='coerce')
        _f['sb_price_diff'] = pd.to_numeric(_f['sb_price_diff'], errors='coerce')
        _f['check_month'] = SETTLE_CK
        _f['run_timestamp'] = run_ts
    _save_delta(_f, ALLOC_FLAGS_TABLE, SETTLE_CK)
    print(f"\nAllocation settlement save complete.")

# COMMAND ----------

# DBTITLE 1,Variable Fee — Parse Invoice
# =============================================================================
# VARIABLE TRADING FEE — PARSE INVOICE
# =============================================================================
if len(vf) == 0:
    print(f"\u26a0\ufe0f No Variable Fee (ZTP Trading) entries in this invoice for {CHECK_MONTH}")
    vf_daily = pd.DataFrame()
else:
    entry = vf[vf['direction'] == 'Entry'].groupby('gas_day').agg(
        entry_qty_mwh=('qty', 'sum'), up_eur_mwh=('up', 'first'),
        entry_amount_eur=('amount', 'sum')).reset_index()
    exit_ = vf[vf['direction'] == 'Exit'].groupby('gas_day').agg(
        exit_qty_mwh=('qty', 'sum'), exit_amount_eur=('amount', 'sum')).reset_index()
    vf_daily = entry.merge(exit_, on='gas_day', how='outer').sort_values('gas_day').reset_index(drop=True)
    vf_daily['total_amount_eur'] = vf_daily['entry_amount_eur'] + vf_daily['exit_amount_eur']
    print(f"\n{'='*70}")
    print(f"VARIABLE FEE (ZTP Trading) \u2014 DAILY ({CHECK_MONTH})")
    print(f"{'='*70}")
    display(vf_daily)
    print(f"\nMonthly: Entry {vf_daily['entry_qty_mwh'].sum():,.1f} MWh | "
          f"Exit {vf_daily['exit_qty_mwh'].sum():,.1f} MWh | EUR {vf_daily['total_amount_eur'].sum():,.2f}")

# COMMAND ----------

# DBTITLE 1,Variable Fee — Equinor ZTPH Volumes
# =============================================================================
# VARIABLE FEE — EQUINOR ZTPH VOLUMES (dispatch + Endurast AST)
# =============================================================================
def get_ztph_shipper_daily(m_start, m_end, mapping_path):
    full_map = pd.read_excel(mapping_path)
    full_map['VALID_FROM'] = pd.to_datetime(full_map['VALID_FROM'])
    full_map['VALID_TO']   = pd.to_datetime(full_map['VALID_TO'])
    ztph_map = full_map[full_map['NODE_ID_DELIVERY'] == 'ZTPH'].copy()
    print(f"Mapping: {len(ztph_map)} ZTPH stems, {ztph_map['SHIPPER_CODE'].nunique()} unique shippers")
    df_raw = spark.table(DISPATCH_TABLE).filter(
        (F.col('country') == COUNTRY) & (F.col('balancing_country') == COUNTRY) &
        (F.col('location_id') == 'ZTPH') &
        (~F.lower(F.col('contract_type')).contains('optimize')) &
        (~F.lower(F.col('contract_type')).contains('balance')) &
        (F.col('gas_day') >= str(m_start)) & (F.col('gas_day') <= str(m_end)) &
        F.col('nomination').isNotNull())
    agg_pd = df_raw.groupBy('gas_day', 'contract_id', 'contract_type', 'quantity_unit').agg(
        F.sum('nomination').alias('volume_kwh')).toPandas()
    agg_pd['gas_day'] = pd.to_datetime(agg_pd['gas_day']).dt.date
    agg_pd['gas_day_ts'] = pd.to_datetime(agg_pd['gas_day'])
    print(f"Dispatch: {len(agg_pd)} contract-day rows")
    if len(agg_pd) == 0: return pd.DataFrame(), agg_pd
    merged = agg_pd.merge(ztph_map[['CONTRACT_GROUP_ID','SHIPPER_CODE','VALID_FROM','VALID_TO']],
        left_on='contract_id', right_on='CONTRACT_GROUP_ID', how='left')
    has_mapping = merged['CONTRACT_GROUP_ID'].notna()
    date_valid = ((merged['VALID_FROM'].isna()|(merged['VALID_FROM']<=merged['gas_day_ts'])) &
                  (merged['VALID_TO'].isna()|(merged['VALID_TO']>=merged['gas_day_ts'])))
    merged = merged[~has_mapping | date_valid].copy()
    unmapped = merged[merged['SHIPPER_CODE'].isna()]['contract_id'].unique().tolist()
    if unmapped: print(f"\u26a0\ufe0f Unmapped contracts: {unmapped}")
    mapped = merged[merged['SHIPPER_CODE'].notna()].copy()
    mapped['signed_kwh'] = mapped['volume_kwh']
    disp_daily = mapped.groupby(['gas_day','SHIPPER_CODE']).agg(
        signed_kwh=('signed_kwh','sum')).reset_index().rename(columns={'SHIPPER_CODE':'ShipperCode'})
    # Endurast AST
    w = Window.partitionBy('counterparty','shippercode','time').orderBy(F.col('enqueued_time').desc())
    df_endu_raw = (spark.table("ms_atlas.endurast_raw.hourly_v1latest")
        .filter((F.col('location')=='ZTPH Hub (H-Zone)')&
                (F.to_date('time')>=str(m_start))&(F.to_date('time')<=str(m_end)))
        .withColumn('rn', F.row_number().over(w)).filter(F.col('rn')==1))
    endu_pre = (df_endu_raw.withColumn('gas_day',F.to_date('time'))
        .select('gas_day','counterparty',F.col('shippercode').alias('ShipperCode'),'unit','quantity').toPandas())
    endu_pre['gas_day'] = pd.to_datetime(endu_pre['gas_day']).dt.date
    agg_endu = endu_pre.groupby(['gas_day','ShipperCode','unit'],as_index=False).agg(signed_kwh=('quantity','sum'))
    print(f"Endurast AST: {len(agg_endu)} shipper-day rows")
    combined = pd.concat([disp_daily[['gas_day','ShipperCode','signed_kwh']],
                          agg_endu[['gas_day','ShipperCode','signed_kwh']]], ignore_index=True)
    shipper_daily = combined.groupby(['gas_day','ShipperCode']).agg(net_kwh=('signed_kwh','sum')).reset_index()
    shipper_daily['position'] = shipper_daily['net_kwh'].apply(
        lambda x: 'NET BUY' if x>0 else ('NET SELL' if x<0 else 'FLAT'))
    return shipper_daily, merged, agg_endu, endu_pre

if len(vf_daily) > 0:
    vf_shipper_daily, _, _, _ = get_ztph_shipper_daily(MONTH_START, MONTH_END, MAPPING_PATH)
    if len(vf_shipper_daily) > 0:
        print(f"\n{'='*75}")
        print(f"ZTPH COMBINED NET POSITIONS \u2014 DAILY ({CHECK_MONTH})")
        print(f"{'='*75}")
        display(vf_shipper_daily.sort_values(['gas_day','ShipperCode']))
    else:
        print(f"\n\u26a0\ufe0f No ZTPH positions found for {CHECK_MONTH}.")
else:
    vf_shipper_daily = pd.DataFrame()
    print("Variable Fee skipped (no invoice entries).")

# COMMAND ----------

# DBTITLE 1,Variable Fee — Compare & Save
# =============================================================================
# VARIABLE TRADING FEE — COMPARE & SAVE
# =============================================================================
if len(vf_daily) == 0 or len(vf_shipper_daily) == 0:
    print("Variable Trading Fee comparison skipped (no data).")
    vf_verdict = 'SKIPPED'; vf_num_flagged = 0
else:
    daily_exit = (vf_shipper_daily[vf_shipper_daily['net_kwh']<0]
        .groupby('gas_day',as_index=False)['net_kwh'].sum()
        .assign(eq_exit_kwh=lambda x: x['net_kwh'].abs()).drop(columns='net_kwh'))
    up_eur_mwh = vf_daily['up_eur_mwh'].iloc[0]
    vf_all_days = pd.DataFrame({'gas_day': pd.date_range(MONTH_START, MONTH_END).date})
    vf_comp = (vf_all_days
        .merge(vf_daily[['gas_day','entry_qty_mwh','exit_qty_mwh','entry_amount_eur','exit_amount_eur','total_amount_eur']], on='gas_day', how='left')
        .merge(daily_exit, on='gas_day', how='left').fillna(0))
    safe_div = lambda a,b: (a/b.replace(0,float('nan'))*100).round(2)
    vf_comp['eq_exit_mwh'] = vf_comp['eq_exit_kwh']/1000.0
    vf_comp['exit_diff_mwh'] = vf_comp['eq_exit_mwh'] - vf_comp['exit_qty_mwh']
    vf_comp['exit_diff_pct'] = safe_div(vf_comp['exit_diff_mwh'], vf_comp['exit_qty_mwh'])
    vf_comp['eq_exit_cost_eur'] = vf_comp['eq_exit_mwh']*up_eur_mwh
    vf_comp['cost_diff_eur'] = vf_comp['eq_exit_cost_eur'] - vf_comp['exit_amount_eur']
    vf_comp['flag'] = ''
    vf_comp.loc[vf_comp['exit_diff_pct'].abs()>1.0, 'flag'] = '\u26a0\ufe0f EXIT'
    vf_flagged = vf_comp[vf_comp['flag']!='']; vf_num_flagged = len(vf_flagged)
    status = '\u2705 ALL MATCH' if vf_num_flagged==0 else f'\u26a0\ufe0f {vf_num_flagged} DAY(S) FLAGGED'
    print(f"\n{'='*90}")
    print(f"VARIABLE TRADING FEE \u2014 DAILY COMPARISON ({CHECK_MONTH})  [{status}]")
    print(f"{'='*90}")
    display(vf_comp[['gas_day','exit_qty_mwh','eq_exit_mwh','exit_diff_pct','exit_amount_eur','eq_exit_cost_eur','cost_diff_eur','flag']])
    inv_exit_total = vf_comp['exit_qty_mwh'].sum()
    eq_exit_total = vf_comp['eq_exit_mwh'].sum()
    exit_vol_diff_pct = (eq_exit_total-inv_exit_total)/inv_exit_total*100 if inv_exit_total else 0
    entry_exit_match = (abs(vf_comp['entry_qty_mwh']-vf_comp['exit_qty_mwh'])<0.01).all()
    eq_grand_total = vf_comp['eq_exit_cost_eur'].sum()*2
    inv_grand_total = vf_comp['total_amount_eur'].sum()
    vf_verdict = 'VERIFIED' if abs(exit_vol_diff_pct)<1.0 else 'REVIEW REQUIRED'
    print(f"\nVerdict: {'\u2705' if vf_verdict=='VERIFIED' else '\u26a0\ufe0f'} {vf_verdict}")
    print(f"Exit vol diff: {exit_vol_diff_pct:+.4f}%  |  Double-charge: {'\u2705' if entry_exit_match else '\u26a0\ufe0f'}")
    # Save
    run_ts = datetime.now()
    print(f"\nSaving Variable Fee results:")
    vf_res = pd.DataFrame([{'check_month':CHECK_MONTH,'category':'ZTP Trading','line_type':'Variable Trading Fee',
        'invoice_file':INVOICE_FILE,'invoice_qty':float(inv_exit_total),'equinor_qty':float(eq_exit_total),
        'qty_diff_pct':float(exit_vol_diff_pct),'invoice_eur':float(inv_grand_total),
        'equinor_eur':float(eq_grand_total),'eur_diff':float(eq_grand_total-inv_grand_total),
        'days_flagged':vf_num_flagged,'price_match':True,'double_charge_confirmed':bool(entry_exit_match),
        'verdict':vf_verdict,'run_timestamp':run_ts}])
    _save_delta(vf_res, VF_RESULTS_TABLE, CHECK_MONTH)
    if vf_num_flagged > 0:
        vf_flags_df = pd.DataFrame([{'check_month':CHECK_MONTH,'gas_day':str(row['gas_day']),
            'category':'ZTP Trading','line_type':'Variable Trading Fee',
            'inv_qty':float(row['exit_qty_mwh']),'eq_qty':float(row['eq_exit_mwh']),
            'qty_diff':float(row['exit_diff_mwh']),'qty_diff_pct':float(row['exit_diff_pct']),
            'inv_up':float(up_eur_mwh),'eq_price':float(up_eur_mwh),'price_match':True,
            'flag':row['flag'].strip(),'run_timestamp':run_ts} for _,row in vf_flagged.iterrows()])
    else:
        vf_flags_df = pd.DataFrame()
    _save_delta(vf_flags_df, VF_FLAGS_TABLE, CHECK_MONTH)
    print("Variable Fee save complete.")

# COMMAND ----------

# DBTITLE 1,Overall Summary
# =============================================================================
# OVERALL SUMMARY
# =============================================================================
print(f"{'='*80}")
print(f"INVOICE CHECKER BELGIUM v2 \u2014 OVERALL SUMMARY")
print(f"{'='*80}")
print(f"Invoice: {INVOICE_FILE}  |  Invoice month: {invoice_month}\n")
ltc_flag_count = len(ltc_flags)
print(f"  {'\u2705' if ltc_flag_count==0 else '\u26a0\ufe0f'} Long Term Capacity: "
      f"{'PASS' if ltc_flag_count==0 else f'{ltc_flag_count} issue(s) flagged'}")
if not HAS_ALLOC:
    print(f"  \u23ed\ufe0f Allocation Settlement: SKIPPED")
else:
    afc = len(alloc_flagged) if isinstance(alloc_flagged, pd.DataFrame) and len(alloc_flagged)>0 else 0
    if not has_final: print(f"  \u23f3 Allocation Settlement ({SETTLE_MONTH_LABEL}): Awaiting final allocations")
    elif afc==0: print(f"  \u2705 Allocation Settlement ({SETTLE_MONTH_LABEL}): PASS")
    else: print(f"  \u26a0\ufe0f Allocation Settlement ({SETTLE_MONTH_LABEL}): {afc} day(s) flagged")
if len(vf)==0: print(f"  \u23ed\ufe0f Variable Trading Fee: SKIPPED")
elif vf_verdict=='SKIPPED': print(f"  \u23ed\ufe0f Variable Trading Fee: SKIPPED (no ZTPH data)")
elif vf_verdict=='VERIFIED': print(f"  \u2705 Variable Trading Fee ({CHECK_MONTH}): PASS")
else: print(f"  \u26a0\ufe0f Variable Trading Fee ({CHECK_MONTH}): {vf_verdict} \u2014 {vf_num_flagged} day(s) flagged")
print(f"\n{'='*80}")
print(f"All results saved to {NEW_SCHEMA}")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Variable Fee — Parse Invoice
# =============================================================================
# VARIABLE TRADING FEE — PARSE INVOICE
# =============================================================================

if len(vf) == 0:
    print(f"\u26a0\ufe0f No Variable Fee (ZTP Trading) entries in this invoice for {CHECK_MONTH}")
    vf_daily = pd.DataFrame()
    vf_monthly = pd.DataFrame()
else:
    # Pivot Entry and Exit into one row per day
    entry = vf[vf['direction'] == 'Entry'].groupby('gas_day').agg(
        entry_qty_mwh=('qty', 'sum'), up_eur_mwh=('up', 'first'),
        entry_amount_eur=('amount', 'sum')
    ).reset_index()
    exit_ = vf[vf['direction'] == 'Exit'].groupby('gas_day').agg(
        exit_qty_mwh=('qty', 'sum'), exit_amount_eur=('amount', 'sum')
    ).reset_index()

    vf_daily = entry.merge(exit_, on='gas_day', how='outer').sort_values('gas_day').reset_index(drop=True)
    vf_daily['total_amount_eur'] = vf_daily['entry_amount_eur'] + vf_daily['exit_amount_eur']

    print(f"\n{'='*70}")
    print(f"VARIABLE FEE (ZTP Trading) \u2014 DAILY ({CHECK_MONTH})")
    print(f"{'='*70}")
    display(vf_daily)

    vf_monthly = pd.DataFrame([{
        'billing_month': CHECK_MONTH,
        'rate_eur_mwh': vf_daily['up_eur_mwh'].iloc[0],
        'entry_qty_mwh': vf_daily['entry_qty_mwh'].sum(),
        'exit_qty_mwh': vf_daily['exit_qty_mwh'].sum(),
        'entry_amount_eur': vf_daily['entry_amount_eur'].sum(),
        'exit_amount_eur': vf_daily['exit_amount_eur'].sum(),
        'total_amount_eur': vf_daily['total_amount_eur'].sum(),
    }])
    print(f"\nMONTHLY SUMMARY:")
    display(vf_monthly)

# COMMAND ----------

# DBTITLE 1,Variable Fee — Equinor ZTPH Volumes
# =============================================================================
# VARIABLE FEE — EQUINOR ZTPH VOLUMES (dispatch + Endurast AST)
# =============================================================================

def get_ztph_shipper_daily(month_start, month_end, mapping_path):
    """Pull ZTPH volumes using NAIVE DATE strategy (no timezone conversion)."""
    full_map = pd.read_excel(mapping_path)
    full_map['VALID_FROM'] = pd.to_datetime(full_map['VALID_FROM'])
    full_map['VALID_TO']   = pd.to_datetime(full_map['VALID_TO'])
    ztph_map = full_map[full_map['NODE_ID_DELIVERY'] == 'ZTPH'].copy()
    print(f"Mapping: {len(ztph_map)} ZTPH stems, {ztph_map['SHIPPER_CODE'].nunique()} unique shippers")

    # --- Pull ZTPH dispatch ---
    df_raw = spark.table(DISPATCH_TABLE).filter(
        (F.col('country') == COUNTRY) & (F.col('balancing_country') == COUNTRY) &
        (F.col('location_id') == 'ZTPH') &
        (~F.lower(F.col('contract_type')).contains('optimize')) &
        (~F.lower(F.col('contract_type')).contains('balance')) &
        (F.col('gas_day') >= str(month_start)) & (F.col('gas_day') <= str(month_end)) &
        F.col('nomination').isNotNull()
    )
    agg_pd = (
        df_raw.groupBy('gas_day', 'contract_id', 'contract_type', 'quantity_unit')
        .agg(F.sum('nomination').alias('volume_kwh')).toPandas()
    )
    agg_pd['gas_day']    = pd.to_datetime(agg_pd['gas_day']).dt.date
    agg_pd['gas_day_ts'] = pd.to_datetime(agg_pd['gas_day'])
    print(f"Dispatch: {len(agg_pd)} contract-day rows, {agg_pd['contract_id'].nunique()} unique contracts")

    if len(agg_pd) == 0:
        return pd.DataFrame(), agg_pd

    # --- Stem + date-validity join ---
    merged = agg_pd.merge(
        ztph_map[['CONTRACT_GROUP_ID', 'SHIPPER_CODE', 'VALID_FROM', 'VALID_TO']],
        left_on='contract_id', right_on='CONTRACT_GROUP_ID', how='left'
    )
    has_mapping = merged['CONTRACT_GROUP_ID'].notna()
    date_valid = (
        (merged['VALID_FROM'].isna() | (merged['VALID_FROM'] <= merged['gas_day_ts'])) &
        (merged['VALID_TO'].isna()   | (merged['VALID_TO']   >= merged['gas_day_ts']))
    )
    merged = merged[~has_mapping | date_valid].copy()
    unmapped = merged[merged['SHIPPER_CODE'].isna()]['contract_id'].unique().tolist()
    if unmapped:
        print(f"\u26a0\ufe0f Unmapped contracts: {unmapped}")

    mapped = merged[merged['SHIPPER_CODE'].notna()].copy()
    mapped['signed_kwh'] = mapped['volume_kwh']
    disp_daily = (
        mapped.groupby(['gas_day', 'SHIPPER_CODE']).agg(signed_kwh=('signed_kwh', 'sum'))
        .reset_index().rename(columns={'SHIPPER_CODE': 'ShipperCode'})
    )

    # --- Endurast AST ---
    ENDURAST_HOURLY_TABLE = "ms_atlas.endurast_raw.hourly_v1latest"
    w_hourly_endu = Window.partitionBy('counterparty', 'shippercode', 'time').orderBy(F.col('enqueued_time').desc())
    df_endu_raw = (
        spark.table(ENDURAST_HOURLY_TABLE)
        .filter((F.col('location') == 'ZTPH Hub (H-Zone)') &
                (F.to_date('time') >= str(month_start)) & (F.to_date('time') <= str(month_end)))
        .withColumn('rn', F.row_number().over(w_hourly_endu)).filter(F.col('rn') == 1)
    )
    endu_pre_agg = (
        df_endu_raw.withColumn('gas_day', F.to_date('time'))
        .select('gas_day', 'counterparty', F.col('shippercode').alias('ShipperCode'), 'unit', 'quantity').toPandas()
    )
    endu_pre_agg['gas_day'] = pd.to_datetime(endu_pre_agg['gas_day']).dt.date
    agg_endu = endu_pre_agg.groupby(['gas_day', 'ShipperCode', 'unit'], as_index=False).agg(signed_kwh=('quantity', 'sum'))
    print(f"Endurast AST: {len(agg_endu)} shipper-day rows, {agg_endu['ShipperCode'].nunique()} unique shippers")

    # --- Combine both sources ---
    combined = pd.concat([
        disp_daily[['gas_day', 'ShipperCode', 'signed_kwh']],
        agg_endu[['gas_day',  'ShipperCode', 'signed_kwh']],
    ], ignore_index=True)
    shipper_daily = combined.groupby(['gas_day', 'ShipperCode']).agg(net_kwh=('signed_kwh', 'sum')).reset_index()
    shipper_daily['position'] = shipper_daily['net_kwh'].apply(
        lambda x: 'NET BUY' if x > 0 else ('NET SELL' if x < 0 else 'FLAT'))
    return shipper_daily, merged, agg_endu, endu_pre_agg

if len(vf_daily) > 0:
    vf_shipper_daily, vf_contract_detail, vf_endurast_detail, vf_endurast_txn = get_ztph_shipper_daily(
        MONTH_START, MONTH_END, MAPPING_PATH)
    if len(vf_shipper_daily) > 0:
        print(f"\n{'='*75}")
        print(f"ZTPH COMBINED NET POSITIONS \u2014 DAILY ({CHECK_MONTH})")
        print(f"{'='*75}")
        display(vf_shipper_daily.sort_values(['gas_day', 'ShipperCode']))
    else:
        print(f"\n\u26a0\ufe0f No ZTPH positions found for {CHECK_MONTH}.")
else:
    vf_shipper_daily = pd.DataFrame()
    print("Variable Fee skipped (no invoice entries).")

# COMMAND ----------

# DBTITLE 1,Variable Fee — Compare & Save
# =============================================================================
# VARIABLE TRADING FEE — COMPARE & SAVE
# =============================================================================
if len(vf_daily) == 0 or len(vf_shipper_daily) == 0:
    print("Variable Trading Fee comparison skipped (no data).")
    vf_verdict = 'SKIPPED'; vf_num_flagged = 0
else:
    daily_exit = (vf_shipper_daily[vf_shipper_daily['net_kwh']<0]
        .groupby('gas_day',as_index=False)['net_kwh'].sum()
        .assign(eq_exit_kwh=lambda x: x['net_kwh'].abs()).drop(columns='net_kwh'))
    up_eur_mwh = vf_daily['up_eur_mwh'].iloc[0]
    vf_all_days = pd.DataFrame({'gas_day': pd.date_range(MONTH_START, MONTH_END).date})
    vf_comp = (vf_all_days
        .merge(vf_daily[['gas_day','entry_qty_mwh','exit_qty_mwh','entry_amount_eur','exit_amount_eur','total_amount_eur']], on='gas_day', how='left')
        .merge(daily_exit, on='gas_day', how='left').fillna(0))
    safe_div = lambda a,b: (a/b.replace(0,float('nan'))*100).round(2)
    vf_comp['eq_exit_mwh'] = vf_comp['eq_exit_kwh']/1000.0
    vf_comp['exit_diff_mwh'] = vf_comp['eq_exit_mwh'] - vf_comp['exit_qty_mwh']
    vf_comp['exit_diff_pct'] = safe_div(vf_comp['exit_diff_mwh'], vf_comp['exit_qty_mwh'])
    vf_comp['eq_exit_cost_eur'] = vf_comp['eq_exit_mwh']*up_eur_mwh
    vf_comp['cost_diff_eur'] = vf_comp['eq_exit_cost_eur'] - vf_comp['exit_amount_eur']
    vf_comp['flag'] = ''
    vf_comp.loc[vf_comp['exit_diff_pct'].abs()>1.0, 'flag'] = '\u26a0\ufe0f EXIT'
    vf_flagged = vf_comp[vf_comp['flag']!='']; vf_num_flagged = len(vf_flagged)
    status = '\u2705 ALL MATCH' if vf_num_flagged==0 else f'\u26a0\ufe0f {vf_num_flagged} DAY(S) FLAGGED'
    print(f"\n{'='*90}")
    print(f"VARIABLE TRADING FEE \u2014 DAILY COMPARISON ({CHECK_MONTH})  [{status}]")
    print(f"{'='*90}")
    display(vf_comp[['gas_day','exit_qty_mwh','eq_exit_mwh','exit_diff_pct','exit_amount_eur','eq_exit_cost_eur','cost_diff_eur','flag']])
    inv_exit_total = vf_comp['exit_qty_mwh'].sum()
    eq_exit_total = vf_comp['eq_exit_mwh'].sum()
    exit_vol_diff_pct = (eq_exit_total-inv_exit_total)/inv_exit_total*100 if inv_exit_total else 0
    entry_exit_match = (abs(vf_comp['entry_qty_mwh']-vf_comp['exit_qty_mwh'])<0.01).all()
    eq_grand_total = vf_comp['eq_exit_cost_eur'].sum()*2
    inv_grand_total = vf_comp['total_amount_eur'].sum()
    vf_verdict = 'VERIFIED' if abs(exit_vol_diff_pct)<1.0 else 'REVIEW REQUIRED'
    print(f"\nVerdict: {'\u2705' if vf_verdict=='VERIFIED' else '\u26a0\ufe0f'} {vf_verdict}")
    print(f"Exit vol diff: {exit_vol_diff_pct:+.4f}%  |  Double-charge: {'\u2705' if entry_exit_match else '\u26a0\ufe0f'}")
    run_ts = datetime.now()
    print(f"\nSaving Variable Fee results:")
    vf_res = pd.DataFrame([{'check_month':CHECK_MONTH,'category':'ZTP Trading','line_type':'Variable Trading Fee',
        'invoice_file':INVOICE_FILE,'invoice_qty':float(inv_exit_total),'equinor_qty':float(eq_exit_total),
        'qty_diff_pct':float(exit_vol_diff_pct),'invoice_eur':float(inv_grand_total),
        'equinor_eur':float(eq_grand_total),'eur_diff':float(eq_grand_total-inv_grand_total),
        'days_flagged':vf_num_flagged,'price_match':True,'double_charge_confirmed':bool(entry_exit_match),
        'verdict':vf_verdict,'run_timestamp':run_ts}])
    _save_delta(vf_res, VF_RESULTS_TABLE, CHECK_MONTH)
    if vf_num_flagged > 0:
        vf_flags_df = pd.DataFrame([{'check_month':CHECK_MONTH,'gas_day':str(row['gas_day']),
            'category':'ZTP Trading','line_type':'Variable Trading Fee',
            'inv_qty':float(row['exit_qty_mwh']),'eq_qty':float(row['eq_exit_mwh']),
            'qty_diff':float(row['exit_diff_mwh']),'qty_diff_pct':float(row['exit_diff_pct']),
            'inv_up':float(up_eur_mwh),'eq_price':float(up_eur_mwh),'price_match':True,
            'flag':row['flag'].strip(),'run_timestamp':run_ts} for _,row in vf_flagged.iterrows()])
    else:
        vf_flags_df = pd.DataFrame()
    _save_delta(vf_flags_df, VF_FLAGS_TABLE, CHECK_MONTH)
    print("Variable Fee save complete.")

# COMMAND ----------

# DBTITLE 1,Variable Fee — Compare & Save
# =============================================================================
# VARIABLE TRADING FEE — COMPARE & SAVE
# =============================================================================

if len(vf_daily) == 0 or len(vf_shipper_daily) == 0:
    print("Variable Trading Fee comparison skipped (no data).")
    vf_verdict = 'SKIPPED'
    vf_num_flagged = 0
else:
    # Aggregate netted SELL volumes per day
    daily_exit = (
        vf_shipper_daily[vf_shipper_daily['net_kwh'] < 0]
        .groupby('gas_day', as_index=False)['net_kwh'].sum()
        .assign(eq_exit_kwh=lambda x: x['net_kwh'].abs())
        .drop(columns='net_kwh')
    )
    up_eur_mwh = vf_daily['up_eur_mwh'].iloc[0]

    vf_all_days = pd.DataFrame({'gas_day': pd.date_range(MONTH_START, MONTH_END).date})
    vf_comp = (
        vf_all_days
        .merge(vf_daily[['gas_day', 'entry_qty_mwh', 'exit_qty_mwh',
                          'entry_amount_eur', 'exit_amount_eur', 'total_amount_eur']],
               on='gas_day', how='left')
        .merge(daily_exit, on='gas_day', how='left')
        .fillna(0)
    )
    safe_div = lambda a, b: (a / b.replace(0, float('nan')) * 100).round(2)

    vf_comp['eq_exit_mwh']      = vf_comp['eq_exit_kwh'] / 1000.0
    vf_comp['exit_diff_mwh']    = vf_comp['eq_exit_mwh'] - vf_comp['exit_qty_mwh']
    vf_comp['exit_diff_pct']    = safe_div(vf_comp['exit_diff_mwh'], vf_comp['exit_qty_mwh'])
    vf_comp['eq_exit_cost_eur'] = vf_comp['eq_exit_mwh'] * up_eur_mwh
    vf_comp['cost_diff_eur']    = vf_comp['eq_exit_cost_eur'] - vf_comp['exit_amount_eur']
    vf_comp['flag'] = ''
    mask_exit = vf_comp['exit_diff_pct'].abs() > 1.0
    vf_comp.loc[mask_exit, 'flag'] = '\u26a0\ufe0f EXIT'

    vf_flagged = vf_comp[vf_comp['flag'] != '']
    vf_num_flagged = len(vf_flagged)
    status = '\u2705 ALL MATCH' if vf_num_flagged == 0 else f'\u26a0\ufe0f {vf_num_flagged} DAY(S) FLAGGED'

    print(f"\n{'='*90}")
    print(f"VARIABLE TRADING FEE \u2014 DAILY COMPARISON ({CHECK_MONTH})  [{status}]")
    print(f"UP: {up_eur_mwh} EUR/MWh (from invoice)")
    print(f"{'='*90}")
    display(vf_comp[['gas_day', 'exit_qty_mwh', 'eq_exit_mwh', 'exit_diff_pct',
                     'exit_amount_eur', 'eq_exit_cost_eur', 'cost_diff_eur', 'flag']])

    # Monthly summary
    inv_exit_total    = vf_comp['exit_qty_mwh'].sum()
    eq_exit_total     = vf_comp['eq_exit_mwh'].sum()
    exit_vol_diff_pct = (eq_exit_total - inv_exit_total) / inv_exit_total * 100 if inv_exit_total else 0
    inv_exit_eur      = vf_comp['exit_amount_eur'].sum()
    eq_exit_eur       = vf_comp['eq_exit_cost_eur'].sum()
    exit_cost_diff    = eq_exit_eur - inv_exit_eur
    vf_verdict        = 'VERIFIED' if abs(exit_vol_diff_pct) < 1.0 else 'REVIEW REQUIRED'

    # Double-charge verification
    entry_exit_match = (abs(vf_comp['entry_qty_mwh'] - vf_comp['exit_qty_mwh']) < 0.01).all()
    eq_grand_total   = eq_exit_eur * 2
    inv_grand_total  = vf_comp['total_amount_eur'].sum()
    grand_total_diff = eq_grand_total - inv_grand_total

    print(f"\n{'='*90}")
    print(f"MONTHLY SUMMARY \u2014 Variable Trading Fee ({CHECK_MONTH})")
    print(f"{'='*90}")
    print(f"  Verdict: {'\u2705' if vf_verdict == 'VERIFIED' else '\u26a0\ufe0f'} {vf_verdict}")
    print(f"  Exit vol diff: {exit_vol_diff_pct:+.4f}%  |  Cost diff: EUR {exit_cost_diff:+,.2f}")
    print(f"  Double-charge: {'\u2705 CONFIRMED' if entry_exit_match else '\u26a0\ufe0f NOT UNIFORM'}")
    print(f"  Est. total (\u00d72): EUR {eq_grand_total:,.2f}  |  Invoice: EUR {inv_grand_total:,.2f}  |  Diff: EUR {grand_total_diff:+,.2f}")

    # --- SAVE ---
    run_ts = datetime.now()
    print(f"\nSaving Variable Fee results:")

    # Results
    vf_res = pd.DataFrame([{
        'check_month': CHECK_MONTH, 'category': 'ZTP Trading', 'line_type': 'Variable Trading Fee',
        'invoice_file': INVOICE_FILE, 'invoice_qty': float(inv_exit_total),
        'equinor_qty': float(eq_exit_total), 'qty_diff_pct': float(exit_vol_diff_pct),
        'invoice_eur': float(inv_grand_total), 'equinor_eur': float(eq_grand_total),
        'eur_diff': float(grand_total_diff), 'days_flagged': vf_num_flagged,
        'price_match': True, 'double_charge_confirmed': bool(entry_exit_match),
        'verdict': vf_verdict, 'run_timestamp': run_ts,
    }])
    _save_delta(vf_res, VF_RESULTS_TABLE, CHECK_MONTH)

    # Flags
    if vf_num_flagged > 0:
        flag_rows = []
        for _, row in vf_flagged.iterrows():
            flag_rows.append({
                'check_month': CHECK_MONTH, 'gas_day': str(row['gas_day']),
                'category': 'ZTP Trading', 'line_type': 'Variable Trading Fee',
                'inv_qty': float(row['exit_qty_mwh']), 'eq_qty': float(row['eq_exit_mwh']),
                'qty_diff': float(row['exit_diff_mwh']), 'qty_diff_pct': float(row['exit_diff_pct']),
                'inv_up': float(up_eur_mwh), 'eq_price': float(up_eur_mwh),
                'price_match': True, 'flag': row['flag'].strip(), 'run_timestamp': run_ts,
            })
        vf_flags_df = pd.DataFrame(flag_rows)
    else:
        vf_flags_df = pd.DataFrame()
    _save_delta(vf_flags_df, VF_FLAGS_TABLE, CHECK_MONTH)
    print(f"Variable Fee save complete.")

# COMMAND ----------

# DBTITLE 1,Overall Summary
# =============================================================================
# OVERALL SUMMARY
# =============================================================================

print(f"{'='*80}")
print(f"INVOICE CHECKER BELGIUM v2 \u2014 OVERALL SUMMARY")
print(f"{'='*80}")
print(f"Invoice: {INVOICE_FILE}")
print(f"Invoice month: {invoice_month}")
print()

# --- LTC ---
ltc_flag_count = len(ltc_flags)
if ltc_flag_count == 0:
    print(f"  \u2705 Long Term Capacity: PASS \u2014 all routes match")
else:
    print(f"  \u26a0\ufe0f Long Term Capacity: {ltc_flag_count} issue(s) flagged")

# --- Allocation Settlement ---
if not HAS_ALLOC:
    print(f"  \u23ed\ufe0f Allocation Settlement: SKIPPED (not in this invoice)")
else:
    alloc_flag_count = len(alloc_flagged) if 'alloc_flagged' in dir() and len(alloc_flagged) > 0 else 0
    if not has_final:
        print(f"  \u23f3 Allocation Settlement ({SETTLE_MONTH_LABEL}): Awaiting final allocations")
    elif alloc_flag_count == 0:
        print(f"  \u2705 Allocation Settlement ({SETTLE_MONTH_LABEL}): PASS")
    else:
        print(f"  \u26a0\ufe0f Allocation Settlement ({SETTLE_MONTH_LABEL}): {alloc_flag_count} day(s) flagged")

# --- Variable Fee ---
if len(vf) == 0:
    print(f"  \u23ed\ufe0f Variable Trading Fee: SKIPPED (not in this invoice)")
elif vf_verdict == 'SKIPPED':
    print(f"  \u23ed\ufe0f Variable Trading Fee: SKIPPED (no ZTPH data)")
elif vf_verdict == 'VERIFIED':
    print(f"  \u2705 Variable Trading Fee ({CHECK_MONTH}): PASS \u2014 {vf_verdict}")
else:
    print(f"  \u26a0\ufe0f Variable Trading Fee ({CHECK_MONTH}): {vf_verdict} \u2014 {vf_num_flagged} day(s) flagged")

print(f"\n{'='*80}")
print(f"All results saved to {NEW_SCHEMA}")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Explore Interruptible Capacity — Invoice
# =============================================================================
# EXPLORE INTERRUPTIBLE CAPACITY — INVOICE (April 2026)
# =============================================================================
import xml.etree.ElementTree as ET
import pandas as pd

tree = ET.parse(INVOICE_PATH)
root = tree.getroot()

# 1. List ALL product names in this invoice
print("ALL PRODUCT NAMES IN INVOICE:")
print("=" * 80)
all_products = []
for prod in root.iter('CustomerProduct'):
    pname = (prod.findtext('ProductName') or '').strip()
    all_products.append(pname)
    marker = " <-- INTERRUPTIBLE" if 'Interruptible' in pname else ""
    print(f"  {pname}{marker}")

# 2. Parse interruptible capacity lines specifically
print(f"\n{'='*80}")
print("INTERRUPTIBLE CAPACITY LINES:")
print("=" * 80)

interruptible_rows = []
for prod in root.iter('CustomerProduct'):
    pname = (prod.findtext('ProductName') or '').strip()
    if 'Interruptible' not in pname:
        continue
    for il in prod.findall('./InvoiceLines/InvoiceLine'):
        for bq in il.findall('.//BilledQuantity'):
            ai = bq.find('AdditionalInformation')
            pfi = bq.find('PriceFormulaInformation')
            qty_el = pfi.find('QTY') if pfi is not None else None
            up_el = pfi.find('UP') if pfi is not None else None
            econ_start = bq.findtext('EconomicStartDate')
            sh_start = int(ai.findtext('ServiceStartGasHour') or 0) if ai is not None else 0
            sh_end = int(ai.findtext('ServiceEndGasHour') or 0) if ai is not None else 0
            service_hours = (sh_end - sh_start + 1) if (sh_start > 0 and sh_end > 0) else 24
            if service_hours <= 0: service_hours += 24
            qty_val = float(qty_el.get('QTY', 0)) if qty_el is not None else None
            interruptible_rows.append({
                'product_name': pname,
                'invoice_line_group': (il.findtext('InvoiceLineGroup') or '').strip(),
                'location': (il.findtext('InvoiceLine') or '').strip(),
                'detail': (il.findtext('InvoiceLineDetail') or '').strip(),
                'billing_start': pd.to_datetime(il.findtext('BillingStartDate')).date() if il.findtext('BillingStartDate') else None,
                'gas_day': pd.to_datetime(econ_start).date() if econ_start else None,
                'direction': ai.findtext('Direction') if ai is not None else None,
                'service_rate_type': ai.findtext('ServiceRateType') if ai is not None else None,
                'contract_ref': ai.findtext('ContractReference') if ai is not None else None,
                'billed_amount': float(bq.findtext('BilledQuantityAmount') or 0),
                'qty_kwh_h': qty_val,
                'service_hours': service_hours,
                'volume_kwh': qty_val * service_hours if qty_val else 0,
                'up': float(up_el.get('UP', 0)) if up_el is not None else None,
                'up_unit': up_el.get('UPUnit', '') if up_el is not None else None,
            })

df_interruptible = pd.DataFrame(interruptible_rows)

if len(df_interruptible) > 0:
    print(f"\nFound {len(df_interruptible)} interruptible line(s)\n")
    
    # Summary by product, direction, location
    int_summary = df_interruptible.groupby(
        ['product_name', 'invoice_line_group', 'direction', 'location', 'service_rate_type'], as_index=False
    ).agg(
        total_billed_eur=('billed_amount', 'sum'),
        total_qty_kwh_h=('qty_kwh_h', 'sum'),
        num_days=('gas_day', 'nunique'),
        avg_up=('up', 'mean'),
        up_unit=('up_unit', 'first'),
        billing_start=('billing_start', 'min'),
        contracts=('contract_ref', lambda x: ', '.join(sorted(set(str(v) for v in x if v)))),
    )
    int_summary['capacity_kwh_h'] = int_summary['total_qty_kwh_h'] / int_summary['num_days']
    display(int_summary)
    
    # Daily detail
    print("\nDaily detail (first 15 rows):")
    display(df_interruptible.head(15))
else:
    print("No interruptible capacity lines found in this invoice.")

# COMMAND ----------

# DBTITLE 1,Explore Interruptible Capacity — Endur
# =============================================================================
# EXPLORE INTERRUPTIBLE CAPACITY — ENDUR
# =============================================================================

# 1. What service_type values exist for Belgium capacity deals?
print("SERVICE TYPES for Belgium capacity deals in Endur:")
print("=" * 80)
df_svc_types = spark.sql("""
    SELECT d.service_type, d.instrument_type_name, COUNT(*) AS deal_count
    FROM ms_atlas.endur_standard.deal_v2latest d
    JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number = pv.deal_number
    JOIN ms_vulcan_gpgtopm_plab.hub2hub_v1.location_country_mapping cm ON pv.location_id = cm.location_id
    WHERE d.instrument_type_name IN ('COMM-CAP-EXIT', 'COMM-CAP-ENTRY')
      AND cm.country_code = 'BE'
      AND d.tran_status = 'Validated'
    GROUP BY d.service_type, d.instrument_type_name
    ORDER BY d.service_type, d.instrument_type_name
""")
display(df_svc_types)

# 2. Look for interruptible deals specifically at Virtualys around April 2026
print("\nINTERRUPTIBLE Belgium capacity deals (if any):")
print("=" * 80)
df_int_endur = spark.sql("""
    SELECT d.deal_number, d.reference, d.service_type, d.instrument_type_name,
           d.buy_sell_name, d.deal_start_date, d.deal_end_date,
           l.location_name, pv.start_date, pv.end_date,
           pv.calculated_profile_volume_kwh, pv.price, pv.settlement_type_id
    FROM ms_atlas.endur_standard.deal_v2latest d
    JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number = pv.deal_number
    JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id = l.location_id
    JOIN ms_vulcan_gpgtopm_plab.hub2hub_v1.location_country_mapping cm ON pv.location_id = cm.location_id
    WHERE d.instrument_type_name IN ('COMM-CAP-EXIT', 'COMM-CAP-ENTRY')
      AND cm.country_code = 'BE'
      AND d.tran_status = 'Validated'
      AND d.service_type != 'Firm'
    ORDER BY d.deal_start_date DESC
    LIMIT 50
""")
print(f"Non-Firm deals found: {df_int_endur.count()}")
display(df_int_endur)

# 3. Also search by reference pattern matching the over-nomination contract
print("\nSearch for SRV-OVERNOM or interruptible references:")
print("=" * 80)
df_overnom = spark.sql("""
    SELECT d.deal_number, d.reference, d.service_type, d.instrument_type_name,
           d.buy_sell_name, d.deal_start_date, d.deal_end_date,
           l.location_name
    FROM ms_atlas.endur_standard.deal_v2latest d
    JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number = pv.deal_number
    JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id = l.location_id
    JOIN ms_vulcan_gpgtopm_plab.hub2hub_v1.location_country_mapping cm ON pv.location_id = cm.location_id
    WHERE cm.country_code = 'BE'
      AND d.tran_status = 'Validated'
      AND (LOWER(d.reference) LIKE '%overnom%' OR LOWER(d.reference) LIKE '%interr%' OR LOWER(d.reference) LIKE '%srv%')
    ORDER BY d.deal_start_date DESC
    LIMIT 50
""")
print(f"Overnom/interruptible reference deals found: {df_overnom.count()}")
if df_overnom.count() > 0:
    display(df_overnom)

# 4. Check if there's a Virtualys entry deal for April 7, 2026 specifically
print("\nVirtualys Entry deals covering April 2026:")
print("=" * 80)
df_virt_apr = spark.sql("""
    SELECT d.deal_number, d.reference, d.service_type, d.instrument_type_name,
           d.buy_sell_name, d.deal_start_date, d.deal_end_date,
           l.location_name, pv.start_date, pv.end_date,
           pv.calculated_profile_volume_kwh, pv.price,
           pv.calculated_profile_volume_kwh / ((DATEDIFF(pv.end_date, pv.start_date) + 1) * 24) AS capacity_kwh_h
    FROM ms_atlas.endur_standard.deal_v2latest d
    JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number = pv.deal_number
    JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id = l.location_id
    WHERE d.instrument_type_name = 'COMM-CAP-ENTRY'
      AND d.tran_status = 'Validated'
      AND LOWER(l.location_name) LIKE '%virtual%'
      AND pv.start_date <= '2026-04-30' AND pv.end_date >= '2026-04-01'
      AND pv.settlement_type_id = 1
    ORDER BY d.service_type, d.deal_start_date
""")
print(f"Virtualys Entry deals for April 2026: {df_virt_apr.count()}")
display(df_virt_apr)

# COMMAND ----------

# DBTITLE 1,Interruptible Capacity — Match & Flag
# =============================================================================
# INTERRUPTIBLE CAPACITY — MATCH & FLAG (testing — no catalog save)
# =============================================================================

# 1. Pull ALL interruptible Endur deals for Belgium covering the invoice period
df_int_endur_match = spark.sql(f"""
    WITH raw AS (
        SELECT d.deal_number, d.reference, d.service_type,
            CASE WHEN d.instrument_type_name='COMM-CAP-ENTRY' THEN 'Entry' ELSE 'Exit' END AS direction,
            l.location_name,
            CAST(DATE_TRUNC('month', pv.start_date) AS DATE) AS billing_month,
            pv.start_date AS pv_start, pv.end_date AS pv_end,
            pv.calculated_profile_volume_kwh AS vol,
            pv.price,
            pv.calculated_profile_volume_kwh / ((DATEDIFF(pv.end_date, pv.start_date) + 1) * 24) AS capacity_kwh_h,
            pv.calculated_profile_volume_kwh * pv.price AS endur_eur,
            ROW_NUMBER() OVER (PARTITION BY d.deal_number, pv.start_date ORDER BY pv.price DESC) AS rn
        FROM ms_atlas.endur_standard.deal_v2latest d
        JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number = pv.deal_number
        JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id = l.location_id
        JOIN ms_vulcan_gpgtopm_plab.hub2hub_v1.location_country_mapping cm ON pv.location_id = cm.location_id
        WHERE d.instrument_type_name IN ('COMM-CAP-EXIT', 'COMM-CAP-ENTRY')
          AND d.service_type = 'Interruptible'
          AND d.tran_status = 'Validated'
          AND pv.settlement_type_id = 1 AND pv.price != 0
          AND cm.country_code = 'BE'
          AND pv.start_date <= '{month_end}' AND pv.end_date >= '{all_month_start}'
    )
    SELECT * FROM raw WHERE rn = 1
""")

int_endur_count = df_int_endur_match.count()
print(f"Interruptible Endur deals covering invoice period ({all_month_start} to {month_end}): {int_endur_count}")
if int_endur_count > 0:
    display(df_int_endur_match.orderBy('pv_start', 'direction', 'location_name'))

# 2. Build comparison: invoice interruptible vs Endur interruptible
print(f"\n{'='*80}")
print("INTERRUPTIBLE CAPACITY — COMPARISON")
print(f"{'='*80}")

if len(df_interruptible) == 0:
    print("No interruptible lines in this invoice.")
else:
    # Map invoice locations to Endur location patterns
    INT_ROUTE_MAP = {
        ('Entry', 'Virtualys'):   {'endur_pattern': '%VIRTUAL%'},
        ('Exit', 'Virtualys'):    {'endur_pattern': '%VIRTUAL%'},
        ('Entry', 'ZPT'):         {'endur_pattern': '%ZBEE%'},
        ('Exit', 'VIP BENE'):     {'endur_pattern': '%VIP_BENE%'},
        ('Entry', 'Zeebrugge'):   {'endur_pattern': '%ZBHUBEE%'},
        ('Exit', 'VIP THE-ZTP'):  {'endur_pattern': '%VIP THE-ZTP%'},
        ('Exit', 'Zeebrugge LNG'):{'endur_pattern': '%Zeebrugge LNG%'},
    }

    # Group invoice interruptible by direction + location + gas_day
    int_inv_daily = df_interruptible.groupby(
        ['direction', 'location', 'gas_day', 'service_rate_type'], as_index=False
    ).agg(
        invoice_kwh_h=('qty_kwh_h', 'sum'),
        invoice_eur=('billed_amount', 'sum'),
        service_hours=('service_hours', 'first'),
        contract_ref=('contract_ref', lambda x: ', '.join(sorted(set(str(v) for v in x if v)))),
        detail=('detail', lambda x: ' | '.join(sorted(set(str(v) for v in x if v)))),
    )

    results = []
    for _, row in int_inv_daily.iterrows():
        d, loc, gd = row['direction'], row['location'], row['gas_day']
        cfg = INT_ROUTE_MAP.get((d, loc))

        endur_match = None
        endur_cap = None
        endur_eur = None
        flag = None

        if cfg and int_endur_count > 0:
            # Try to match on direction + location pattern + date
            matches = df_int_endur_match.filter(
                (F.col('direction') == d) &
                (F.lower(F.col('location_name')).like(cfg['endur_pattern'].lower())) &
                (F.col('pv_start') <= F.lit(str(gd))) &
                (F.col('pv_end') >= F.lit(str(gd)))
            ).collect()

            if matches:
                endur_cap = sum(abs(m['capacity_kwh_h']) for m in matches)
                endur_eur = sum(abs(m['endur_eur']) for m in matches)
                endur_match = ', '.join(m['reference'] for m in matches)
                gap_pct = abs(row['invoice_kwh_h'] - endur_cap) / row['invoice_kwh_h'] * 100 if row['invoice_kwh_h'] else 0
                flag = 'MATCHED' if gap_pct < 2 else f'CAPACITY GAP ({gap_pct:.1f}%)'
            else:
                flag = 'NO ENDUR MATCH — likely over-nomination'
        else:
            flag = 'NO ENDUR MATCH — likely over-nomination'

        results.append({
            'direction': d,
            'location': loc,
            'gas_day': gd,
            'rate_type': row['service_rate_type'],
            'service_hours': row['service_hours'],
            'invoice_kwh_h': row['invoice_kwh_h'],
            'invoice_eur': row['invoice_eur'],
            'endur_kwh_h': endur_cap,
            'endur_eur': endur_eur,
            'endur_reference': endur_match,
            'contract_ref': row['contract_ref'],
            'detail': row['detail'],
            'flag': flag,
        })

    df_int_results = pd.DataFrame(results)
    display(df_int_results)

    # Summary
    print(f"\n{'='*80}")
    print("INTERRUPTIBLE SUMMARY")
    print(f"{'='*80}")
    total_inv = df_int_results['invoice_eur'].sum()
    matched = df_int_results[df_int_results['flag'] == 'MATCHED']
    unmatched = df_int_results[df_int_results['flag'] != 'MATCHED']
    print(f"  Total interruptible invoiced: €{total_inv:,.2f}")
    print(f"  Matched to Endur:  {len(matched)} line(s)")
    print(f"  Unmatched/flagged: {len(unmatched)} line(s)")
    for _, u in unmatched.iterrows():
        print(f"    ⚠️ {u['direction']} {u['location']} ({u['gas_day']}): {u['invoice_kwh_h']:,.0f} kWh/h, €{u['invoice_eur']:,.2f} — {u['flag']}")
        print(f"       Contract: {u['contract_ref']} | Detail: {u['detail']}")