# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

CITY_MAPPING = {
    # الرياض
    'الرياض': 'Riyadh', 'رياض': 'Riyadh', 'riyadh': 'Riyadh', 'الدرعية': 'Ad Diriyah', 'ad diriyah': 'Ad Diriyah',
    'الخرج': 'Al Kharj', 'al-kharj': 'Al Kharj', 'المجمعة': 'Al Majmaah', 'حوطة سدير': 'Hawtat Sudair',
    'ضرما': 'Duruma', 'مرات': 'Marat', 'حريملاء': 'Huraymila', 'الدوادمي': 'Ad Duwadimi', 'الدلم': 'Al-Dalm', 'نفي': 'Nifi',
    # مكة وجدة والغربية
    'جدة': 'Jeddah', 'جده': 'Jeddah', 'jeddah': 'Jeddah', 'مكة': 'Makkah', 'مكة المكرمة': 'Makkah', 'مكه': 'Makkah', 'mecca': 'Makkah',
    'المدينة': 'Medina', 'المدينة المنورة': 'Medina', 'medina': 'Medina', 'al madinah': 'Medina', 'ينبع': 'Yanbu',
    'الطائف': 'Taif', 'رابغ': 'Rabigh', 'الجموم': 'Al Jumum', 'الحناكية': 'Al Hinakiyah', 'العلا': 'Al Ula', 'خليص': 'Khulays',
    'القنفذة': 'Al Qunfidhah', 'al-qunfudhah': 'Al Qunfidhah', 'الليث': 'Al-Lith', 'مدينة الملك عبدالله الاقتصادية': 'King Abdullah Economic City',
    # الشرقية
    'الدمام': 'Dammam', 'al damam': 'Dammam', 'dammam': 'Dammam', 'الخبر': 'Khobar', 'al khobar': 'Khobar', 'الظهران': 'Dhahran',
    'الجبيل': 'Jubail', 'jubail': 'Jubail', 'الأحساء': 'Al Ahsa', 'الاحساء': 'Al Ahsa', 'الهفوف': 'Al Hofuf', 'al-hafof': 'Al Hofuf',
    'المبرز': 'Al Mubarraz', 'القطيف': 'Qatif', 'سيهات': 'Sayhat', 'بقيق': 'Buqayq', 'abqaiq': 'Buqayq', 'رأس تنورة': 'Ras Tannurah',
    'الخفجي': 'Al-Khafgy', 'حفر الباطن': 'Hafar Al-Batin', 'النعيرية': 'Nariyah', 'عنك': 'Inak', 'قرية العليا': 'Qaryat Al Ulya',
    'سلوى': 'Salwa', 'العيون': 'Al Uyun', 'الشرقية': 'Eastern Province', 'المنطقة الشرقية': 'Eastern Province', 'eastern': 'Eastern Province',
    # القصيم
    'القصيم': 'Qassim', 'qassim': 'Qassim', 'بريدة': 'Buraydah', 'عنيزة': 'Unaizah', 'البكيرية': 'Al Bukayriyah', 'المذنب': 'Al Midhnab', 'أوثال': 'Awthal',
    # الجنوب
    'أبها': 'Abha', 'خميس مشيط': 'Khamis Mushait', 'عسير': "'Asir", 'asir': "'Asir", 'أحد رفيدة': 'Ahad Rafidah', 'النماص': 'An Namas',
    'بيشة': 'Bisha', 'الحرجة': 'Al Harajah', 'المجاردة': 'Al-Magerda', 'جازان': 'Jizan', 'جيزان': 'Jizan', 'sabya': 'Sabya',
    'أبو عريش': 'Abu Arish', 'أحد المسارحة': 'Ahad Al Musarihah', 'العارضة': 'Al Aridah', 'الدرب': 'Al-Darb', 'نجران': 'Najran', 'الباحة': 'Al Bahah', 'بلجرشي': 'Biljurashi',
    # الشمال
    'تبوك': 'Tabuk', 'حقل': 'Haql', 'ضباء': 'Duba', 'الوجه': 'Al Wajh', 'أملج': 'Umluj', 'أمالا': 'Amaala', 'حائل': 'Hail',
    'الجوف': 'Al Jawf', 'سكاكا': 'Sakaka', 'دومة الجندل': 'Dawmat Al Jandal', 'القريات': 'Al Qurayyat', 'عرعر': 'Arar', 'رفحاء': 'Rafha', 'الحدود الشمالية': 'Northern Borders'
}

# COMMAND ----------

# MAGIC %md
# MAGIC **Clean Description**

# COMMAND ----------

def clean_html_spark(col):
    return F.trim(F.regexp_replace(
        F.regexp_replace(col, r"<[^>]+>", " "),
        r"\s+", " "
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC **Map City**

# COMMAND ----------

def map_city_py(val):
    if not val: return 'Unknown'
    v = str(val).strip()
    if v.lower() in ['n/a', 'unknown', 'none', '', 'nan']:
        return 'Unknown'
    return CITY_MAPPING.get(v.lower(), CITY_MAPPING.get(v, v.title()))

map_city_udf = F.udf(map_city_py, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC **Standardize Date**

# COMMAND ----------

def parse_date_spark(col):
    c = F.trim(col)

    ts_iso = F.when(
        c.contains("T"),
        F.to_timestamp(F.regexp_replace(F.substring(c, 1, 19), "T", " "), "yyyy-MM-dd HH:mm:ss")
    )

    ts_ymd = F.when(
        c.rlike(r"^\d{4}-\d{2}-\d{2}"),
        F.to_timestamp(F.substring(c, 1, 19), "yyyy-MM-dd HH:mm:ss")
    )

    part1 = F.split(c, "/").getItem(0).cast("int")
    
    ts_dmy_slash = F.when(
        c.rlike(r"^\d{1,2}/\d{1,2}/\d{4}") & (part1 > 12),
        F.to_timestamp(F.substring(c, 1, 19), "dd/MM/yyyy HH:mm:ss")
    )

    ts_mdy_slash = F.when(
        c.rlike(r"^\d{1,2}/\d{1,2}/\d{4}") & (part1 <= 12),
        F.to_timestamp(F.substring(c, 1, 19), "MM/dd/yyyy HH:mm:ss")
    )

    ts_dmy_slash_date = F.when(
        c.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$") & (part1 > 12),
        F.to_timestamp(c, "dd/MM/yyyy")
    )
    ts_mdy_slash_date = F.when(
        c.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$") & (part1 <= 12),
        F.to_timestamp(c, "MM/dd/yyyy")
    )

    ts_epoch_ms = F.when(c.rlike(r"^\d{13}$"), F.to_timestamp(F.from_unixtime(c.cast("double") / 1000.0)))
    ts_epoch_s = F.when(c.rlike(r"^\d{10}$"), F.to_timestamp(F.from_unixtime(c.cast("double"))))

    final_ts = F.coalesce(
        ts_iso,
        ts_ymd,
        ts_dmy_slash,
        ts_mdy_slash,
        ts_dmy_slash_date,
        ts_mdy_slash_date,
        ts_epoch_ms,
        ts_epoch_s,
        F.to_timestamp(c)
    )

    return F.date_format(final_ts, "yyyy-MM-dd HH:mm:ss")

# COMMAND ----------


print("Function are defined and ready!")