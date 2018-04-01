# Twitter Analysis with Hadoop

## Preparación del entorno

Para realizar la compilación del código, es necesario instalar Maven. A continuación, se detalla su proceso de instalación en Ubuntu.

```{bash}
sudo apt-get update
sudo apt-get install maven
```

Una vez intalado, sería adecuado comrpobar si la instalación de maven ha sido satisfactoria. Además del comando, se muestran la versiones de Maven y Java con las que se ha construido el .jar adjunto.

```{bash}
mvn -version

# Apache Maven 3.0.5
# Maven home: /usr/share/maven
# Java version: 1.7.0_55, vendor: Oracle Corporation
# Java home: /usr/lib/jvm/java-7-openjdk-amd64/jre
# Default locale: en_US, platform encoding: UTF-8
# OS name: "linux", version: "3.13.0-24-generic", arch: "amd64", family: "unix"
```

Ahora podemos compilar nuestro código, de forma que se genere una carpeta `target` con dos archivos .jar. Uno de ellos únicamente contiene el ejecutable del código, el otro, además, contiene las dependencias. Este último es el que utilizaremos.

```{r}
mvn clean
mvn package
```

El siguiente paso es poner el fichero de entrada en HDFS. Para ello crearemos una carpeta `tweets`, en donde colocaremos dicho archivo.

```{r}
hadoop fs -mkdir tweets
```

Ejecutando el siguiente comando deberíamos visualizar la nueva carpeta creada.

```{r}
hadoop fs -ls
```

Finalmente, añadimos el archivo cache-1000000-json.gz a HDFS.

```{bash}
hadoop fs -put <ruta-al-archivo> tweets
```

# Ejecución

El código se ha escrito para ejecutar sobre la siguiente versión de Hadoop:

```{bash}
hadoop version

# Hadoop 2.4.0.2.1.3.0-563
# Subversion git@github.com:hortonworks/hadoop.git -r # 5538b411baa6785ff91062f77b8d36d2828d6073
# Compiled by jenkins on 2014-06-26T22:07Z
# Compiled with protoc 2.5.0
# From source with checksum ba30e572b67b51c2a94485e328ea8
# This command was run using /usr/lib/hadoop/hadoop-common-2.4.0.2.1.3.0-563.jar
```

Para ejecutar el código será suficiente con el siguiente comando (se sobreentiende que nos encontramos en la raíz del proyecto).

```{bash}
hadoop jar target/twitter_analysis-1.0-SNAPSHOT-jar-with-dependencies.jar tweets/cache-1000000-json.gz tweets/out
```

Una vez ejecutada la aplicación, comprobamos que se hayan generado los archivos en el directorio de salida `tweets/out`.

```{bash}
hadoop fs -ls tweets/out
```

Finalmente, juntamos todos los archivos parciales en un *csv*.

```{bash}
hadoop fs -getmerge -nl tweets/out stats.csv
```

## Cuestiones sobre resultados

A continuación de detallan las estadísicas obtenidas para dos de los usuarios:

- 00ASHLEYMARIE00: 
    - Número de tweets: 19
    - Volume momentum: 0.5941058941058941
    - Popularity momentum: 0.0
- 007_Debby: 
    - Número de tweets: 26
    - Volume momentum: 0.5898701985005119
    - Popularity momentum: 0.0

## Diseño de Arquitectura

