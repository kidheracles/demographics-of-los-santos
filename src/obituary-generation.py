#!/usr/bin/env python
# coding: utf-8

# # Cleaning up [00 Exploration.ipynb](00_Exploration.ipynb)

# *** 
# _00 Exploration_ is the rough draft where we generated the texts for [@evryCalifornian](twitter.com/evryCalifornian)
# 
# In this notebook, we will condense and clean that process.

# ***
# __Set up__

# In[1]:


import pandas as pd # for data analysis
import gzip         # to work with zip files 
import spacy        # for NLP (dealing with occupations)

# In[2]:


# to keep datafiles small work with the gzip file
with gzip.open("../data/raw/usa_00035.dta.gz", "rb") as file:
    df = pd.read_stata(file)
    
df.shape
df.head()

# there are 137 columns so it's better to print them out this way
print(list(df.columns))


# ***
# __Cleaning up__
# 
# We need to deal with `"countyfips"` and `"occ"` codes

# For `"countyfips"` we have the csv file `county_fips.csv`

# In[3]:


county_fips = pd.read_csv("../data/raw/county_fips.csv")

county_fips['fips'] = county_fips['fips'].astype(int) # to match df's

county_fips.head()


# In[4]:


fips_codes = county_fips[['fips', 'county']].to_dict(orient = 'records')

ls_codes_fips = []

for code in fips_codes:
    ls_codes_fips.append(list(code.values()))
    
    fips = {item[0]: item[1] for item in ls_codes_fips}


# In[5]:


fips


# Now we can `map` those values to `df['countyfips']`

# In[6]:


df['countyfips'] = df['countyfips'].map(fips)

# fill null values with a string value so you can manipulate the series more easily later on
df['countyfips'] = df['countyfips'].fillna(value = 'N/A')


# ***
# This same process we will repeat with `df['occ']`
# ***

# In[7]:


occ = pd.read_csv("../data/raw/OCC2016.csv")

occ['ACS'] = occ['ACS'].astype(int)
occ.head()


# In[8]:


occ_codes = occ[['ACS', 'Occupation Name']].to_dict(orient = 'records')

ls_codes_occ = []
for code in occ_codes:
    ls_codes_occ.append(list(code.values()))
    
    occupations = {item[0]: item[1] for item in ls_codes_occ}


# In[9]:


df['occ'] = df['occ'].map(occupations)

df['occ'] = df['occ'].fillna(value = 'N/A')


# ***
# ### Natural Language Processing using SpaCy
# 
# _Occupation codes_ are generalizations of individuals' occupations. For example, an individual with OCC code 50 has an occupation label of _Marketing and sales managers_ right now. In our tweets we want to speak in the first-person so the tweet would say something like ___"I'm a marketing and sales manager"___. This requires a little bit of NLP. 
# 
# Enter, SpaCy...

# In[10]:


# import spacy      # already imported at the beginning of the notebook but uncomment if needed
nlp = spacy.load("en_core_web_sm")


# ***
# ___quick note___: visit [spacy's documentation](https://spacy.io/usage/models) to learn about loading a model. For this example we're using the english language model but they have more languages!
# ***

# The following piece of code was found [here in stackoverflow](https://stackoverflow.com/a/44764557).

# More on POS-tagging: https://spacy.io/api/annotation#section-pos-tagging

# In[24]:


get_ipython().run_cell_magic(u'time', u'', u"lemma = []\ntags = []\nnnps = []\n\n# Because we're only looking to create a occupation label for those who actually have a job\ndata = df[df['empstat'] == 'employed'].copy()\n\nok_tags = ['JJ', 'JJR', 'JJS', 'NN','NNS', 'NNPS'] # adjectives and nouns\n\n\n# this might take a while if you have a long dataframe\nfor doc in nlp.pipe(data['occ'].astype('unicode').values, batch_size = 50, n_threads=3):\n    if doc.is_parsed:\n        lemma.append([n.lemma_ for n in doc])   # all words lemmatized\n        tags.append([n.lemma_ for n in doc if n.tag_ in ok_tags])    # lemmatized words only if nouns or adjectives\n        nnps.append([n.lemma_ for n in doc if n.tag_ == 'NNPS'])     # proper nouns\n    else:\n        # To make sure the list lemma and tag are the same length as the series OCC\n        lemma.append(None)\n        tags.append(None)\n        nnps.append(None)\n        \n\ndata['occ_lemma'] = lemma\ndata['occ_tag'] = tags\ndata['occ_nnps'] = nnps")


# ~9 minutes for 173,342 rows.

# At this point `data['occ_lemma']` and `data['occ_tag']` are series of lists which cannot be saved in `.dta` files. We'll have to change them to `strings`.

# In[25]:


data['occ_tag'] = data['occ_tag'].apply(lambda x: ', '.join(map(str, x)))
data['occ_lemma'] = data['occ_lemma'].apply(lambda x: ', '.join(map(str, x)))
data['occ_nnps'] = data['occ_nnps'].apply(lambda x: ', '.join(map(str, x)))


# ***
# ### At this point we have a working semi-cleaned dataset and we could save it as a checkpoint.

# In[26]:


working_df = pd.merge(df,data,how = 'left')


# Since we are hosting this on github we'll try to keep the size to <25mb so we'll drop some columns:
# 1. year: it's 2016 ACS data so we don't need a variable for year... it's 2016.
# 2. gqtype and gqtyped: group quarters type and type detailed are probably not going to be interesting enough to tweet about it
# 3. hhwt and perwt: household weight and person weight could be interesting. Maybe something like "there are about {person[perwt]} people like me in CA". So you could keep it if that's something that interests you.
# 4. statefips: it's california. 
# 5. homeland: only tells you wether your puma (public use microdata area) contains a person's homeland
# 6. puma, migpuma1, pwpuma00: this is 2016's puma code, if you want to use these you could use it to have a very specific area where this person would be tweeting from

# In[27]:


# dropping these columns will get you just under 25mb
working_df.drop(columns = ['year', 'gqtype', 'gqtyped', 'hhwt', 'statefip', 'homeland', 'puma', 'pwpuma00', 'migpuma1'], inplace = True)

# remove unused categories in your series
for col in df.columns:
    if str(df[col]) == 'category':
        df[col].cat.remove_unused_categories()

# The original file obtained from IPUMS is in .dta format so we'll keep it that way
# it also conserves categoricals, which is useful.
with gzip.open("../data/processed/working-101718_dataset.dta.gz", "wb") as file:
    working_df.to_stata(file, write_index = False)


# ***
# # To be continued tomorrow:
# create sentence fragments from columns

# In[105]:


# loading from check-point
with gzip.open("../data/processed/working-101718_dataset.dta.gz", "rb") as datafile:
    working_df = pd.read_stata(datafile)
    
working_df.head()


# ***
# Because we will not be using every single one of these 130 columns we can start dropping some. <br>
# The following I'll choose based on what I want my twitterbot to tweet, you may choose to keep whatever variable you're interested in if you are going to be using this dataset as well.

# In[106]:


# We can make a list of variables to drop
income_vars = [col for col in working_df.columns if "inc" in col]

income_vars.remove("incwage") # we want to keep these
income_vars.remove("inctot")

working_df.drop(columns=income_vars, inplace = True)

# Repeat the process for other groups of variables
vet_vars = [col for col in working_df.columns if "vet" in col]

vet_vars.remove("vetstat")

working_df.drop(columns=vet_vars, inplace = True)

# randoms
other_vars = ['lingisol','city','multgend','ind','bpld','uhrswork','yrnatur', 'citizen','yrimmig','availble', 'foodstmp','marrno', 'divinyr', 'widinyr','wkswork2','mortgage', 'degfield', 'rentmeal','gq', 'degfield2','ownershp', 'ownershpd', 'mortgag2', 'farmprod', 'acrehous', 'mortamt1', 'mortamt2', 'rentgrs', 'fridge', 'hotwater', 'bedrooms', 'phone', 'cinethh', 'cilaptop', 'cismrtphn', 'citablet', 'ciothcomp', 'cidatapln', 'fuelheat', 'nfams', 'nsubfam', 'ncouples', 'birthyr', 'raced', 'race', 'hispan', 'hispand', 'ancestr1', 'ancestr2', 'languaged', 'educ', 'gradeatt', 'schltype', 'degfieldd', 'degfield2d', 'empstatd', 'classwkr', 'classwkrd', 'migrate1d', 'movedin']

working_df.drop(columns=other_vars, inplace = True)

# cost
cost_vars = [col for col in working_df.columns if 'cost' in col]

working_df.drop(columns=cost_vars, inplace = True)

# health insurance
health_vars = [col for col in working_df.columns if 'hins' in col]

working_df.drop(columns=health_vars, inplace = True)


# ***
# You can save this trimmed dataset and start working on building your sentences from it.

# In[131]:


with gzip.open("../data/processed/working-101818-cleaned_dataset.dta.gz", "wb") as file:
    working_df.to_stata(file, write_index = False)


# ## Constructing sentences
# 
# Based on the variables left I put came up with 11 different categories.
# 
# 1. Demographics:
#   - countyfips, sex, age, marst, yrmarr,
# 2. Household:
#   - farm, rent, vehicles, ssmc, multgen
# 3. Work:
#   - empstat, labforce, occ, looking, pwstate2, occ_lemma, occ_tag, occ_nnps
# 4. Origin
#   - bpl, ancestr1d, ancestr2d, yrsusa1
# 5. Language
#   - language
# 6. Health coverage:
#   - hcovany
# 7. Education
#   - educd, gradeattd
# 8. Money
#   - inctot, incwage, poverty
# 9. Moving
#   - migrate1, migplac1
# 10. Veteran
#   - vetstat
# 11. Commute
#   - tranwork, carpool, riders, trantime, departs, arrives

# Based on these categories we can create 11 potential sentence fragments. Of course, not all observations will have all 11 fragments.
# 
# Before moving to the code itself it's a good idea to map out the logic for each fragment in ___pseudo-code___:

# ###### Demographics
# 
# `countyfips`, `age`, and `sex` are values we can expect from every observation so we can build a sentence from there. The other variables _could or could not_ have values depending on whether a person is married or not (`marst`).
# 
# An example sentence:<br>
# ```python
# sentence = "I'm {age}, from {countyfips}"
# if sex == 'male':
#     sentence += man emoji
# else:
#     sentence += woman emoji
# 
# if age >= 18:
#     if marst == "never married/single":
#         sentence = sentence + ". I'm single"
#     elif "married" in marst:
#         sentence += "I got married in {yrmarr}"
#     else:
#         sentence += first word of marst ## divorced, separated, or widowed.
# else:
#     pass
# ```
# 
# So you end up with either <br>
# _"I'm 16 {emoji}, from San Diego county"_ or <br>
# _"I'm 34 {emoji}, from Alameda county. I got married in 2007."_ or <br>
# _"I'm 40 {emoji}, from Los Angeles county. I'm divorced."_

# ###### Household
# All variables in the Household category are _conditional_
# 
# ```python
# if farm == 'farm':
#     sentence = 'I live in a farm! {farmer emoji}'
# else:
#     sentence = ""
# 
# if rent >= 0:
#     sentence += "I pay {rent} in rent."
# else:
#     sentence += ""
#     
# if vehicles > "1 available":
#     sentence += "I have a car available {car emoji}"
# else:
#     sentence += ""
#     
# if ssmc != "households without a same-sex married couple":
#     sentence += "{rainbow emoji}"
# else:
#     sentence += ""
#     
# if multgen == "2 generations" | "3+ generations":
#     sentence += "more than 1 generation lives in my home."
# else:
#     sentence += ""
# ```
# 
# In some cases you'll end up with a blank string for sentence but in others you may potentially end up with a 4 part sentence: <br>_"I live in a farm! {farmer emoji}. I pay {rent} in rent. I have a car available {car emoji}. {rainbow}. More than 1 generation lives in my home."_

# ###### Work
# The work sentence is a little more complicated. We used spacy to create `occ_lemma`, `occ_tag`, and `occ_nnps`. From which we can create a "job title" label but for the rest we can create other fragments.
# foodstmp, empstat, labforce, occ, uhrswork, looking, availble, pwstate2, occ_lemma, occ_tag, occ_nnps
# 
# ```python
# if empstat == 'unemployed':
#     sentence = "I'm unemployed"
#     if looking == 'yes, looked for work':
#         sentence += ", but I'm still looking for a job."
#     else:
#         sentence += "."
# elif empstat == 'employed':
#     sentence = "I work as {job label from occ_lemma or occ_tag}"
#     if pwstate2 != 'n/a' & pwstate2 != 'california':
#         sentence += " in {pwstate2}."
#     else:
#         pass
# else:
#     sentence = ""
#     
# ```
# 
# So you end up with something like:<br>
# _"I'm unemployed, but I'm still looking for a job."_ or <br>
# _"I'm unemployed."_ or <br>
# _"I work as a scientist in Canada."_

# ###### Origin
# The origin sentence is a little more straight-forward.
# 
# ```python
# sentence = "I was born in {bpl}."
# if ancestr1d != "not classified" | "other" | "not reported":
#     sentence += "I am {ancestr1d}"
#     if ancestr2d != "not classified" | "other" | "not reported":
#         sentence += " and {ancestr2d}."
#     else:
#         sentence += "."
# else:
#     pass
# ```

# ###### Language and health coverage
# These are simple straight-forward sentences:
# ```python
# if language != "other or not reported":
#     sentence = "I speak {language} at home."
# else:
#     sentence = ""
#     
# if hcovany == "with health insurance coverage":
#     sentence = "I have health insurance."
# else:
#     sentence = "I don't have health insurance."
# ```

# ###### Education
# ```python
# if age < 18:
#     sentence = "I am in {gradeattd}."
# ```
