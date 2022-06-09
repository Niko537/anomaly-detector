from clickhouse_driver import Client
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans


con = Client(
    'host',
    user='user',
    password='password'
)

class DataProcesser:
  def __init__(self, query):
    """
    query : string
      query to clickhouse database
    """
    ret = con.execute(query)
    qq = pd.DataFrame(ret)
    names = ['DAY', 'METRICS_NAME', 'VALUE']
    qq.columns = names
    qq['DAY'] = pd.to_datetime(qq['DAY'], format='%Y%m%d', errors='ignore')
    qq['VALUE'] = qq['VALUE'].astype(int)
    self.data = qq
  
  def get_value_names(self):
    return set(list(self.data['METRICS_NAME']))
  
  def get_series(self, target_name):
    series = []
    for i in range(len(self.data)):
      if target_name == self.data.iloc[i]['METRICS_NAME']:
        series.append(self.data.iloc[i]['VALUE'])
    return series
  
  def detect_anamolies(self, target_name, alpha=1):
    
    series = []
    anomalies_tmp = {}

    for i in range(len(self.data)):
      if target_name == self.data.iloc[i]['METRICS_NAME']:
        series.append(self.data.iloc[i]['VALUE'])
        anomalies_tmp[self.data.iloc[i]['DAY']] = self.data.iloc[i]['VALUE']
    
    anomalies = {}
    

    series = np.array(series)

    std = series.std()
    mean = series.mean()
    treshold = std*alpha

    lower = mean - treshold
    upper = mean + treshold

    for c_n, x_t in anomalies_tmp.items():
      if x_t > upper or x_t < lower:
        anomalies[c_n] = x_t
    return anomalies
  
  def visualize(self, target_name, mean = False, clusters = False):

    mean : bool
      if true than plot will apear with the mean line

    clusters : bool
      if true than plot will apear with clusters

    series = []
    for i in range(len(self.data)):
      if target_name == self.data.iloc[i]['METRICS_NAME']:
        series.append(self.data.iloc[i]['VALUE'])
    series = np.array(series)

    fig = plt.figure()
    fig.suptitle(target_name)

    if mean == True:
      plt.plot(range(len(series)), [series.mean()]*len(series))
    
    if clusters == True:
      clf = KMeans(n_clusters=3)
      clf.fit([(x,y) for x,y in enumerate(series)])
      y = clf.predict([(x,y) for x,y in enumerate(series)])
      y0 = [series[i] for i in range(len(y)) if y[i] == 0]
      y1 = [series[i] for i in range(len(y)) if y[i] == 1]
      y2 = [series[i] for i in range(len(y)) if y[i] == 2]

      plt.scatter(range(len(y0)), y0)
      plt.scatter(range(len(y1)), y1)
      plt.scatter(range(len(y2)), y2)
    else:
      plt.scatter(range(len(series)), series)

    plt.show()
