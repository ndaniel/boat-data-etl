#!/usr/bin/env python
# coding: utf-8

# In[816]:


import numpy as np
import pandera as pa
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import collections
import unicodedata
import re
from datetime import datetime
get_ipython().run_line_magic('matplotlib', 'inline')


# In[817]:


fn = "boat_data.csv"
d = pd.read_csv(fn,encoding="utf-8")
d.info()
d.head()


# In[818]:


display(d.sample(5))


# In[819]:


print(d.shape)


# In[820]:


def only_ascii(text,special=False):
    # keep only ASCII characters (0-127)
    if special: # save the pound sign as GBP
        text = text.replace("£","GBP")
        text = text.replace('Â»','|')
    x = ''.join(c if ord(c)<128 else " " for c in text)
    while True:
        x = x.replace("  "," ")
        x = x.replace(" ,",",")
        x = x.replace(' "','"')
        x = x.replace('" ','"')
        if x.find("  ")==-1:
            break
    x = x.strip()
    return x


# In[821]:


# this is a temporary file such that it is easier to debug if something goes wrong
fn2= fn.replace(".csv", "_ascii.csv")

# Read input file
text = None
with open(fn, "r", encoding="utf-8") as f:
    lines = [line.rstrip("\r\n") for line in f if line.rstrip("\r\n")]
    text = "\n".join(lines)
    # clean the non-ASCII characters
    r = [only_ascii(e,special=True) for e in lines]


with open(fn2, "w", encoding="latin1") as f:
    f.writelines([e+"\n" for e in r])



# In[822]:


# now pandas can read the file
d = pd.read_csv(fn2,encoding="latin1")


# In[823]:


print(d.shape)


# In[824]:


d.info()
d.head()


# In[825]:


d.isna().sum()


# In[826]:


x = d["Price"].str.partition(" ")
d["Currency"] = x[0]
d["Price"] = x[2].astype(int)


# In[827]:


def euro(price,currency):
    if currency=='EUR':
        return price
    elif currency=='CHF':
        return int(price) * 1.06
    elif currency=='DKK':
        return int(price) * 0.13
    elif currency=='GBP':
        return int(price) * 1.17


# In[828]:


d["Euro"] = d.apply(lambda x: euro(x["Price"], x["Currency"]), axis=1)


# In[829]:


d.sample(5)


# In[ ]:





# In[830]:


# see the histogram for the years
YEAR_COL = "Year Built"
d[YEAR_COL].value_counts().sort_index()

# for the year built use the min year - 10 where is NaN 
m = min([e for e in d[YEAR_COL] if e and e!=0])
m2 = m - 10

current_year = datetime.now().year
d[YEAR_COL]=d[YEAR_COL].apply(lambda x: x if m <=x and x <= current_year else m2)

d[YEAR_COL].value_counts().sort_index()


# In[831]:


d[YEAR_COL].plot.hist(bins=30)
plt.xlabel("year")
plt.ylabel("count")
plt.title("histo")
plt.show()


# In[832]:


d[YEAR_COL].value_counts().sort_index()


# In[833]:


#Split Location to 
x = d['Location'].str.split("|",n=1,expand=True)
d['Country'] =x[0].str.rstrip()
d['City'] = x[1].str.rstrip() 
d.drop(columns=['Location'], inplace=True)
x


# In[834]:


display(d.sample(5))


# In[835]:


d["Country"].unique()


# In[836]:


d["Country"] = d["Country"].astype(str).str.strip().str.lower()


# In[837]:


replace_country = {
    # Valid countries (normalized)
    "switzerland": "Switzerland",
    "germany": "Germany",
    "denmark": "Denmark",
    "italy": "Italy",
    "france": "France",
    "united kingdom": "United Kingdom",
    "spain": "Spain",
    "austria": "Austria",
    "netherlands": "Netherlands",
    "slovenia": "Slovenia",
    "serbia": "Serbia",
    "slovakia": "Slovakia",
    "croatia": "Croatia",
    "portugal": "Portugal",
    "malta": "Malta",
    "montenegro": "Montenegro",
    "latvia": "Latvia",
    "greece": "Greece",
    "poland": "Poland",
    "turkey": "Turkey",
    "finland": "Finland",
    "hungary": "Hungary",
    "cyprus": "Cyprus",
    "czech republic": "Czech Republic",
    "sweden": "Sweden",
    "lithuania": "Lithuania",
    "united states": "United States",
    "ukraine": "Ukraine",
    "estonia": "Estonia",
    "monaco": "Monaco",
    "russia": "Russia",
    "egypt": "Egypt",
    "united arab emirates": "United Arab Emirates",
    "australia": "Australia",
    "bulgaria": "Bulgaria",
    "philippines": "Philippines",
    "taiwan": "Taiwan",
    "thailand": "Thailand",
    "luxembourg": "Luxembourg",
    "venezuela": "Venezuela",
    "ireland": "Ireland",
    "norway": "Norway",
    "seychelles": "Seychelles",
    "morocco": "Morocco",
    "lebanon": "Lebanon",
    "romania": "Romania",

    # Localized or typo versions of countries
    "italien": "Italy",
    "italie": "Italy",
    "dalmatien": "Croatia",
    "kroatien krk": "Croatia",
    "espa?a": "Spain",

    # Cities/regions mapped to countries
    "steinwiesen": "Germany",
    "rolle": "Switzerland",
    "baden baden": "Germany",
    "lake constance": "Germany",
    "split": "Croatia",
    "lago maggiore": "Italy",
    "brandenburg an derhavel": "Germany",
    "zevenbergen": "Netherlands",
    "faoug": "Switzerland",
    "martinique": "France",
    "gibraltar": "United Kingdom",
    "mallorca": "Spain",
    "opwijk": "Belgium",
    "isle of man": "United Kingdom",
    "neusiedl am see": "Austria",
    "bodensee": "Germany",
    "avenches": "Switzerland",
    "heilbronn": "Germany",
    "z richse, 8855 wangen sz": "Switzerland",
    "ibiza": "Spain",
    "lommel": "Belgium",
    "wijdenes": "Netherlands",
    "bremen": "Germany",
    "bielefeld": "Germany",
    "porto rotondo": "Italy",
    "berlin wannsee": "Germany",
    "toscana": "Italy",
    "vierwaldst ttersee - buochs": "Switzerland",
    "juelsminde havn": "Denmark",
    "barssel": "Germany",
    "welschenrohr": "Switzerland",
    "thun": "Switzerland",
    "adria": "Italy",
    "rovinij": "Croatia",                            # city in Croatia
    "donau": "Germany",                              # Danube river (German name)
    "travem nde": "Germany",                         # typo for Travemünde, Germany
    "stralsund": "Germany",                          # city in Germany
    "rostock": "Germany",                            # city in Germany
    "lake geneva": "Switzerland",                    # lake on Swiss–French border
    "belgi, zulte": "Belgium",                       # town in Belgium
    "niederrhein": "Germany",                        # region in Germany
    "r gen": "Germany",                              # typo for Rügen island
    "oder": "Germany",                               # river
    "beilngries": "Germany",                         # town in Bavaria
    "marina punat": "Croatia",                       # marina on Krk island
    "french southern territories": "France",         # overseas territory
    "brandenburg": "Germany",                        # state in Germany
    "nan": "None",                                # NaN entry
    "waren m ritz": "Germany",                       # Waren (Müritz), town in Germany
    "jersey": "United Kingdom",                      # British crown dependency
    "neustadt in holstein (ostsee)": "Germany",      # town in northern Germany
    "ostsee": "Germany",                             # Baltic Sea (German name)
    "greetsile/ krummh rn": "Germany",               # Greetsiel / Krummhörn, Lower Saxony
    "annecy": "France",                              # city in France
    "izola": "Slovenia",                             # coastal town
    "83278 traunstein": "Germany",                   # German town (postal code)
    "novi vinodolski": "Croatia",                    # coastal town
    "lago di garda": "Italy",                        # lake in northern Italy
    "nordseek ste": "Germany",                       # typo for Nordseeküste (North Sea coast)
    "24782 b delsdorf": "Germany",                   # Büdelsdorf, Germany
    "pt stkysten ellers esbjerg": "Denmark",         # likely Esbjerg (coastal Denmark)
    "calanova mallorca": "Spain",                    # marina in Mallorca
    "katwijk": "Netherlands",                        # coastal town
    "tenero, lago maggiore": "Switzerland",          # lakeside town
    "fu ach": "Austria",                             # typo for Fußach, Austria
    "angera": "Italy",                               # lakeside town
    "lago maggiore, minusio": "Switzerland",         # lakeside town
    "thalwil": "Switzerland",                        # suburb of Zürich
    "rheinfelden": "Germany"                         # could also be Switzerland, but likely German side
}





# In[838]:


d["Country"] = d["Country"].replace(replace_country)
d["Country"].unique()


# In[839]:


d.isna().sum()


# 

# In[840]:


# Length - processing
sns.histplot(data=d, x="Length", bins=30, kde=True)
plt.xlabel("Length")
plt.ylabel("Count")
plt.title("Histogram of Length")
plt.show()


# In[841]:


d['Length'] = d['Length'].fillna(0)


# In[842]:


# Width - processing
sns.histplot(data=d, x="Width", bins=30, kde=True)
plt.xlabel("Width")
plt.ylabel("Count")
plt.title("Histogram of Width")
plt.show()


# In[843]:


d['Width'] = d['Width'].fillna(0)


# In[844]:


d.isna().sum()


# In[845]:


d['Type'] =d["Type"].fillna('None')


# In[846]:


d.isna().sum()


# In[847]:


d["Type"].unique()


# In[848]:


x = d["Type"].tolist()
x


# In[849]:


x = [e.partition(",") for e in x]
x


# In[850]:


power = [e[2] if e[2] else 'None' for e in x]
power


# In[851]:


x = [e[0] for e in x]
x


# In[852]:


d["Type"]=x


# In[853]:


d["Power"]=power


# In[854]:


d.isna().sum()


# In[855]:


d['Manufacturer']=d['Manufacturer'].fillna('None')


# In[856]:


d.isna().sum()


# In[857]:


d["Material"].value_counts().sort_index()


# In[858]:


d['Material']=d['Material'].fillna('None')
d['City']=d['City'].fillna('None')
d['Country']=d['Country'].fillna('None')


# In[859]:


d.isna().sum()


# In[860]:


d.dtypes


# # Exploratory Data Analysis (EDA)

# In[861]:


# correlations
corr = d.select_dtypes(include='number').corr()
sns.heatmap(corr, annot=True)
plt.show()


# In[ ]:


# correlation matrix when the categorical variables are encoded
d_encoded = d.copy()
for col in d.select_dtypes(include='object'):
    d_encoded[col] = d[col].astype('category').cat.codes

corr = d_encoded.corr().round(2)
sns.heatmap(corr, annot=True,annot_kws={"size": 8})
plt.show()


# In[863]:


d.isna().sum()


# In[866]:


fig = plt.figure(figsize=(10,6) )
Country_View= d.groupby(['Country','Power'])['Number of views last 7 days'].mean().reset_index(drop=False).sort_values(by=['Number of views last 7 days','Country'],ascending=False)
sns.barplot(x='Country', y='Number of views last 7 days',hue='Power', data=Country_View)
plt.title("Average views last 7 days per Country and Engine Type")
plt.xticks(rotation=70)
plt.legend(loc = 'center left', bbox_to_anchor=(1,0.5), title = 'Boat Type')


# In[867]:


# Compute average views per Country + Power
Country_View = (
    d.groupby(['Country', 'Power'])['Number of views last 7 days']
    .mean()
    .reset_index()
)

# Compute total views per country (ignoring engine type)
top_countries = (
    Country_View.groupby("Country")['Number of views last 7 days']
    .mean()
    .nlargest(10)
    .index
)

# Filter Country_View to include only top 10 countries
Country_View = Country_View[Country_View["Country"].isin(top_countries)]

# Sort by total views (for better display)
Country_View["Country"] = pd.Categorical(
    Country_View["Country"],
    categories=top_countries,
    ordered=True
)

# Plot
fig = plt.figure(figsize=(10, 6))
sns.barplot(
    x='Country',
    y='Number of views last 7 days',
    hue='Power',
    data=Country_View
)
plt.title("Average views last 7 days per Country and Engine Type")
plt.xticks(rotation=70)
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), title='Engine Type')
plt.tight_layout()
plt.show()


# In[871]:


Material_View= d.groupby('Material')['Number of views last 7 days'].mean().sort_values().reset_index(drop=False)


sns.barplot(x='Material', y='Number of views last 7 days', data=Material_View)
plt.xticks(rotation=70)
plt.title("Average views last 7 days per Material")


# In[872]:


d.to_csv("validated_boat_data.csv", index=False)


# In[ ]:




