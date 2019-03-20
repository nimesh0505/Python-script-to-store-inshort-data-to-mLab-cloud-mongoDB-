import urllib.request,json
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dateutil.parser import parse
from datetime import date
from datetime import datetime
#from beautifultable import BeautifulTable
list_category=['national','business','sports','world','politics','technology','startup','entertainment','miscellaneous','science','automobile']
for n in range(len(list_category)):
	with urllib.request.urlopen("http://127.0.0.1:5000/news?category="+list_category[n]) as url:
		data=json.loads(url.read().decode())
		b=[]
		b=data['data']

	list_aut=[]
	list_content=[]
	list_imageUrl=[]
	list_readMoreUrl=[]
	list_time=[]
	list_hours=[]
	list_mins=[]
	list_title=[]
	list_url=[]
	list_days=[]
	list_month=[]
	list_year=[]
	list_onlydate=[]
	temp_date=[]
	list_fdate=[]
	analyser = SentimentIntensityAnalyzer()
	def sentiment_analyzer_scores(sentence):
	    score = analyser.polarity_scores(sentence)
	    return score
	a=len(data['data'])
	category=list_category[n]
	myclient =MongoClient("mongodb://nemo:nemo1234@ds137634.mlab.com:37634/cloud_all_inshort")
	db=myclient.cloud_all_inshort
	if(db.cloud_final_db_converted.count()==0):
		for i in range(a):
			list_aut.append(b[i]['author'])
			list_content.append(b[i]['content'])
			ti=datetime.strptime(b[i]['time'],'%I:%M %p')
			convert_time=ti.strftime('%H:%M')
			temp=str(b[i]['date'])
			lenf=len(temp)
			index=temp.find(",")
			list_time.append(convert_time)
			list_days.append(temp[(index+1):lenf])
			temp_date.append(temp[0:(index)])
			dt=parse(str(temp_date[i]))
			list_fdate.append((dt.strftime('%Y-%m-%d')))
			list_year.append(dt.strftime('%Y'))
			list_month.append(dt.strftime('%m'))
			list_onlydate.append(dt.strftime('%d'))
			list_imageUrl.append(b[i]['imageUrl'])
			list_readMoreUrl.append(b[i]['readMoreUrl'])
			list_hours.append(ti.strftime('%H'))
			list_mins.append(ti.strftime('%M'))
			list_title.append(b[i]['title'])
			list_url.append(b[i]['url'])

		for x in range(len(list_aut)):
			temp=sentiment_analyzer_scores(list_content[x])
			pos=temp['pos']
			neg=temp['neg']
			neu=temp['neu']
			compound=temp['compound']

			business={
				'author' : list_aut[x],
				'content' : str(list_content[x]).lower(),
				'c_date': parse(list_fdate[x]).strftime('%Y-%m-%d'),
				'imageUrl' : list_imageUrl[x],
				'readMoreUrl' : list_readMoreUrl[x],
				'time' : parse(list_time[x]).strftime('%H:%M'),
				'category':list_category[n],
				'title' : str(list_title[x]).strip(),
				'url' : list_url[x],
				'positive' :pos,
				'negative' : neg,
				'neutral' : neu,
				'compound' : compound,
				'year' : int(list_year[x]),
				'months' : int(list_month[x]),
				'date' : int(list_onlydate[x]),
				'day' : list_days[x],
				'hours' : int(list_hours[x]),
				'minutes' : int(list_mins[x]),
				}
			db.cloud_final_db_converted.insert_one(business)

	else:
		for i in range(a):
			list_aut.append(b[i]['author'])
			list_content.append(b[i]['content'])
			ti=datetime.strptime(b[i]['time'],'%I:%M %p')
			convert_time=ti.strftime('%H:%M')
			temp=str(b[i]['date'])
			lenf=len(temp)						
			index=temp.find(",")
			list_days.append(temp[(index+1):lenf])
			temp_date.append(temp[0:(index)])
			dt=parse(str(temp_date[i]))
			list_fdate.append((dt.strftime('%Y-%m-%d')))
			list_year.append(dt.strftime('%Y'))
			list_month.append(dt.strftime('%m'))
			list_onlydate.append(dt.strftime('%d'))
			list_imageUrl.append(b[i]['imageUrl'])
			list_readMoreUrl.append(b[i]['readMoreUrl'])
			list_hours.append(ti.strftime('%H'))
			list_mins.append(ti.strftime('%M'))
			list_time.append(convert_time)
			list_title.append(b[i]['title'])
			list_url.append(b[i]['url'])
		for x in range(a):

			temp=sentiment_analyzer_scores(list_content[x])
			pos=temp['pos']
			neg=temp['neg']
			neu=temp['neu']
			compound=temp['compound']

			db.cloud_final_db_converted.update_one({'title' : list_title[x]},{"$set":{'author' : list_aut[x],
			'content' : str(list_content[x]).lower(),
			'c_date': parse(list_fdate[x]).strftime('%Y-%m-%d'),
			'imageUrl' : list_imageUrl[x],
			'readMoreUrl' : list_readMoreUrl[x],
			'time' : parse(list_time[x]).strftime('%H:%M'),
			'category':list_category[n],
			'url' : list_url[x],
			'positive' :pos,
			'negative' : neg,
			'neutral' : neu,
			'compound' : compound,
			'year' : int(list_year[x]),
			'months' : int(list_year[x]),
			'date' : int(list_onlydate[x]),
			'day' : list_days[x],
			'hours':int(list_hours[x]),
			'minutes':int(list_mins[x])},},upsert=True)