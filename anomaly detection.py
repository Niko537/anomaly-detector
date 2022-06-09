from anomaly_detector import anomaly_detector as ad

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

# Тестовый забор данных временного ряда по различным метрикам
dp = ad.DataProcesser(f'''select f.IDAY as DAY,
       													 d.CNAME as NAME,
       													 sum(f.NVALUE) as VALUE
													from VEGA_DS.TBL_F_BASIC_METRICS f
         												join VEGA_DS.TBL_D_BASIC_METRICS d on d.CCODE = f.CCODE
													where toDate(parseDateTimeBestEffort(toString(f.IDAY))) >= today() - 30
													group by DAY,
         													 NAME
													order by DAY''')

# преобразовываем наименование метрик в читаемый вид
values = [i for i in dp.get_value_names()] 
values = '\n'.join(values)
print(color.GREEN + color.BOLD + 'Имена наших показателей: ' + color.END + '\n' + values + '\n')

# получаем значение временной ряд по конкретной метрике
series = dp.get_series('Выручка без НДС, руб.')
print(color.GREEN + color.BOLD +'Количество значений: ' + color.END + color.YELLOW + str(len(series)) + color.END + '\n')
print(color.GREEN + color.BOLD + 'Все значения метрики Выручка без НДС, руб. за выбранный период: '  + color.END + str(series) + '\n')

# детекция выбросов с задаваемым порогов чувствительности alpha
# чем больше alpha, тем менее чувствительна к выбросам модель
outliers = dp.detect_anamolies('Выручка без НДС, руб.', alpha=2)
outliers1 = []
for key, val in outliers.items():
   outliers1.append(color.CYAN + color.BOLD + str(key) + ': ' + color.END + color.RED + str('{:,}'.format(val).replace(',', ' ')) + color.END)

outliers2 = '\n'.join(outliers1)

print(color.GREEN + color.BOLD +'Количество аномальных значений: ' + color.END + color.RED + str(len(outliers)) + color.END + '\n')
print(color.GREEN + color.BOLD +'Найденные аномалии в метрике Выручка без НДС, руб. за ранее определенный нами период: ' + color.END + '\n' + str(outliers2))