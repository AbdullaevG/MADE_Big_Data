### Homework_01


1. **Задания уровня beginner**
   - Пробросить порт (port forwarding) для доступа к HDFS Web UI;
   - Воспользоваться Web UI для того, чтобы найти папку “/backup_virtual” в HDFS, а в ней логи сервиса - “access_log”. Посмотрите в Web UI, сколько 
      подпапок в папке “/backup_virtual/access_logs” без учета рекурсии? (в ответе ожидается одно число);

     
2. **Задания уровня intermediate**
   - Вывести  рекурсивно список всех файлов в /data/wiki;
   - См. п.1 + вывести размер файлов в “human readable” формате (т.е. не в байтах, а, например, в МБ, когда размер файла измеряется от 1 до 1024 МБ);
   - Ответьте на вопрос: какой фактор репликации используется для файлов? В случае работы с Docker-контейнером, к ответу прибавьте 2. (в ответе ожидается одно число);
   - Ответьте на вопрос: какой фактор репликации используется для папок? (в ответе ожидается одно число);
   - Команда "hdfs dfs -ls" выводит актуальный размер файла (actual) или же объем пространства, занимаемый с учетом всех реплик этого файла (total)?В ответе ожидается одно слово: actual или total;
   - Приведите команду для получения размера пространства, занимаемого всеми файлами (с учетом рекурсии, но без учета фактора репликации) внутри “/data/wiki”. На выходе ожидается одна строка с указанием команды;
   - Чтобы избежать конфликтов, создайте папку в домашней HDFS-папке Вашего пользователя. На всякий случай используйте Ваш id (см. таблицу с оценками) в качестве префикса этой папки;
   - Создайте вложенную структуру из папок глубины 3 одним вызовом CLI. Символы ‘;’ и ‘&’ в команде запрещены. Решить задачу нужно не объединением нескольких команд в одну строку, а вызовом одной команды;
   - Удалите созданные папки рекурсивно;
   - Что такое Trash в распределенной FS (ответ текстом)? Как сделать так, чтобы файлы удалялись сразу, минуя “Trash” (указать команду)?
   - Создайте пустой файл в HDFS;
   - Создайте небольшой произвольный файл (идеально - 15 строчек по 100 байт) и загрузите файл из локальной файловой системы (local FS)4 в HDFS;
   - Выведите содержимое HDFS-файла на экран;
   - Выведите конец HDFS-файла на экран;
   - Выведите содержимое нескольких первых строчек HDFS-файла на экран;
   - Разберитесь в чем разница между HDFS флагом "-tail" и локальной утилитой "tail". С помощью какой команды (флага) можно воспроизвести поведение HDFS "-tail" локально?;
   - Сделайте копию файла в HDFS;
   - Переместите копию файла в HDFS на новую локацию;
   - Загрузите HDFS-файлы локально5, объединив их в один файл во время загрузки одним вызовом CLI;



3. **Задания уровня advanced**
   - Изменить replication factor для файла (команда). Как долго занимает время на увеличение / уменьшение числа реплик для файла (текст/обсуждение в чатах)?
   - Найдите информацию по файлу, блокам и их расположениям с помощью “hdfs fsck” CLI;
   - Получите информацию по любому блоку из п.2 с помощью "hdfs fsck -blockId”. Обратите внимание на Generation Stamp (GS number);
   - Выберите произвольный файл в HDFS;
     - Воспользовавшись знаниями из предыдущих 3 заданий, найдите информацию, из каких блоков состоит этот файл;
     - Смените пользователя на hdfsuser , перейдите 6 на одну из Datanode и найдите физическую реплику одного любого блока, выбранного hdfs-файла, на этой Datanode. Выведите содержимое этого блока. Обратите внимание где и как хранится метаинформация по этой реплике;
     - Слепок файловой структуры Namenode (e.g. edits.log) предоставлен в HDFS по адресу “/data/namenode_example”. Выведите содержимое этой папки. Внутри неё найдите и выведите snapshot (fsimage) и транзакции сервиса Namenode;


**Задачи на работу с сервисом WebHDFS:**
  - С помощью WebHDFS, используя параметры запроса, прочитайте 100B из произвольного файла в HDFS;
  - Научитесь пользоваться опцией “follow redirects” с помощью curl (см. “man curl”). Прочитайте произвольный файл из HDFS с использованием этой опции;
  - Используя параметры запроса, получите детализированную информацию по файлу (см. file status);
  - Используя параметры запроса, измените параметр репликации файла;
  - Используя параметры запроса, дозапишите данные в файл (append). Решение должно быть реализовано с помощью одного curl-запроса;