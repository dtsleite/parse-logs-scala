### Respostas para a primeira parte do questionário

- ##### Qual o objetivo do comando cache em Spark?
R:  O cache no Spark tem como objetivo permitir o acesso mais rápido aos datasets sendo utilizados múltiplas vezes evitando que seja feita uma reavaliação todas vez que seja invocada uma action.
- ##### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
R: Comparando com MapReduce temos uma performance 100x mais rápida tratando-se de processamento em memória e 10x mais rápida utilizando o disco. Isso se dá porque as principais operações do Spark são alocadas em memória.
- ##### Qual é a função do SparkContext ?
R: O SparkContext é o ponto de entrada para aplicações Spark, representa um encapsulamento com todas as funcionalidades do Spark  e a conexão entre a "aplicação driver" e o cluster sendo utilizado.
- ##### Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
R:  Um RDD é uma abstração para estrutura de dados no Spark, é imutável, distribuído, tolerante a falhas e processados paralelamente através do cluster.
- ##### GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
R: GroupByKey aumenta o tráfego de dados na rede devido ao seu modo de realizar o shuffle que é aplicado em todos os pares chave-valor em cada cluster. O ReduceByKey realiza o shuffle somente depois de combinar os pares chave-valor na máquina local.
- ##### Explique o que o código Scala abaixo faz.
<pre>
// carrega um arquivo de texto existente no hdfs
val textFile = sc.textFile("hdfs://...")
// transforma o arquivo em campos utilizando como split o " " (espaço)
val counts = textFile.flatMap(line => line.split(" "))
// cria a estrutura chave,valor para futura utilização (em uma contagem)
.map(word => (word,1))
// agrupa os valores totalizados (item, total)
.reduceByKey(\_+\_)
// grava o resultado da contagem em um local no hdfs com a seguinte estrutura
// ._SUCESS
//.part-nnnn
.counts.saveAsTextFile("hdfs://...")
</pre>

### Respostas para a segunda parte do teste aplicado
O código fonte para atender esta requisição encontra-se neste repositório, foi criado utilizando o Spark 2.3 e Scala 2.11.

##### São dois os arquivos necessários:
ParseLog.scala; responsável por realizar o tratamento dos dados existentes nos arquivos de log a fim de ser um utilitário para a classe de extração de informações.

NasaChallenge.scala; responsável por extrair as informações dos arquivos de log e apresentá-las.

##### Os resultados obtidos no processamento são mostrados abaixo:

Quantidade de hosts únicos: 137978

Total de erros 404: 20901

Top 5 urls com erros 404: 
(/pub/winvn/readme.txt,2004)  
(/pub/winvn/release.txt,1732)  
(/shuttle/missions/STS-69/mission-STS-69.html,683)  
(/shuttle/missions/sts-68/ksc-upclose.gif,428)  
(/history/apollo/a-001/a-001-patch-small.gif,384)  

Média de erros 404 por dia: 360.36  
Total de bytes returnados: 65524314915
