## HW #05: Spark RDD

В данном ДЗ нужно решить 2 задачи. Для решения использовать только Apache
Spark RDD API. 

Цель: разобраться в принципах работы c RDD

 1. Задача #1 (Task ID: spark.bigram): народные биграммы
    
    Найдите все пары двух последовательных слов (биграмм), где первое слово: `narodnaya`. Для каждой пары подсчитайте количество вхождений в тексте статей Википедии.
Выведите все пары с их частотой вхождений в лексикографическом порядке. Формат вывода: `word_pair <tab> count`

    Путь на кластере: полный датасет - `/data/wiki/2 en_articles_part`
    
    Формат: текст
    
    В каждой строке находятся следующие поля, разделенные знаком табуляции:
     - INT - id статьи
     - STRING - текст статьи
 
    Требования к реализации:
     - PySpark-скрипты для запуска решений следует называть  `task_<Surname>_<Name>_bigram.py`;
     - запустить с помощью команды: `PYSPARK_DRIVER_PYTHON=python3.6 PYSPARK_PYTHON=python3.6 spark-submit "task_<Surname>_<Name>_bigram.py"`
     - id статьи не учитывать в вычислениях;
     - для однозначности вычислений, выделить слова из статьи (включая название статьи) с помощью регулярного выражения re.findall(r"\w+", text);
     - стоп-слова не фильтровать;
     - привести все слова к нижнему регистру;
     - слова в паре объединить символом нижнего подчеркивания “_”;
     - отсортировать слова в выводе (STDOUT) по алфавиту;
     - Вывод (STDOUT) сохранить в файл: `task_<Surname>_<Name>_bigram.out`;
     - Условие быстродействия: решение должно отрабатывать в течение 3х минут на свободном кластере (3 ноды x 8 CPU x 16GB RAM).
   
3. Задача #2 (Task ID: spark.collocation): коллокации

   Необходимо найти топ коллокаций в Википедии по метрике NPMI. Коллокация - это комбинации слов, которые часто встречаются вместе. Например: «high school» или «Roman Empire». Чтобы определить, является ли пара слов коллокацией,
воспользуйтесь метрикой NPMI - нормализованная точечная взаимная информация. $$PMI(a,b) = \log{\frac{P(ab)}{P(a)P(b)}}$$ $$NPMI(a,b) = \frac{PMI(a,b)}{-\log{P(ab)}}$$
Путь на кластере
   - stop_words_en:   `/data/stop_words/stop_words_en-xpo6.txt (формат: одно стоп-слово на строчку)`
   - полный датасет - `/data/wiki/2 en_articles_part`

   Требования к реализации:
   - PySpark-скрипты для запуска решений следует называть  `task_<Surname>_<Name>_collocation.py`;
   - запустить с помощью команды: `PYSPARK_DRIVER_PYTHON=python3.6 PYSPARK_PYTHON=python3.6 spark-submit "task_<Surname>_<Name>_collocation.py"`
   - id статьи не учитывать в вычислениях;
   - для однозначности вычислений, выделить слова из статьи (включая название статьи) с помощью регулярного выражения re.findall(r"\w+", text);
   - привести все слова к нижнему регистру;
   - удалить стоп-слова до расчёта значений P(a) и до объединения слов в биграммы (то есть строка “how to tie a bow tie” должна быть преобразована в пару биграмм [“tie_bow”, “bow_tie”]);
   - отфильтровать биграммы, которые встретились не реже 500 раз (т.е. проводим все необходимые join'ы и считаем NPMI только для них, НО оценку вероятности встретить биграмму считаем на полном датасете). Общее число биграмм вычисляется после фильтрации стоп слов;
   - отсортировать слова в выводе по значению NPMI;
   - вывести (STDOUT ) TOP-39 отсортированных по NPMI самых популярных коллокаций и их значения NPMI (округляем до 3-го знака после запятой, см. round);
   - Вывод (STDOUT) сохранить в файл: `task_<Surname>_<Name>_collocation.out`
  

  
      
    
