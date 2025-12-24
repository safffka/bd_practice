## Lab 1
``` bash
cd lab01
chmod +x run.sh
./run.sh WAP12.txt
```
Результат в терминале
## Lab 2
``` bash
cd lab02
docker build  -t lab02 .
docker run lab02 > out.txt
```
Результат в файле out.txt

## Lab 3
``` bash
cd lab03
docker build  -t lab03 .
docker run lab03 > out.txt
```
Результат в файле out.txt

## Lab 4
``` bash
cd lab04
docker build  -t lab04 .
docker run lab03 > out.txt
```
Результат в файле out.txt

## Lab 5
``` bash
cd lab05
docker build  -t lab05 .
docker run lab05
```
Результат в терминале

## Lab 6
``` bash
cd lab06
docker build  -t lab06 .
docker run lab06
docker exec -it lab06-postgres-source-1 psql -U postgres
\dt
```
Результат в терминале