Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

## Tabla de contenidos

1. [Identificación de peticiones](#identificación-de-peticiones)
2. [Protocolo de comunicación](#protocolo-de-comunicación)
3. [Coordinación de instancias](#coordinación-de-instancias)
4. [Escalabilidad](#escalabilidad)

## Identificación de peticiones

Para poder identificar las peticiones de los clientes se definio un identificador `req_id` que acompaña los mensajes del cliente a lo largo de todo el sistema. Entonces, todas las instancias (_gateway_, _sum_, _aggregation_, _join_) envian a la siguiente instancia del sistema un mensaje que contiene el `req_id` asociado al cliente que se le esta resolviendo la petición.

> [!NOTE]
> Actualmente el `req_id` se define a partir del identificador del objeto de Python que se utiliza para enviar mensajes a la _queue_, como se crea una instancia por cada cliente que se conecta al _gateway_, entonces se cumple que el `req_id` es único por cada cliente.

## Protocolo de comunicación

Para poder comunicar las instancias entre si de forma robusta, se definio un protocolo de comunicación que consiste en indicar, mediante un número, que tipo de mensaje es el que se esta enviando a la otra instancia.

> [!NOTE]
> Cada uno de estos mensajes viene acompañado por un `req_id`, de esta forma se puede identificar a que cliente corresponden.

* `0 = EOF` se utiliza para indicar que no se van a recibir mas datos.
* `1 = DATA` se utiliza para indicar que se esta enviando datos, el tipo de datos depende de que instancia lo envie.
* `2 = PREPARE` se utiliza solamente por las instancias de _sum_, para que la _instancia coordinadora_ le pregunte a las demas instancias de _sum_ cuantos mensajes recibieron y procesaron para un cliente particular.
* `3 = OK` se utiliza solamente por las instancias de _sum_, para que las demas instancias de _sum_ le indiquen la cantidad de mensajes que recibieron y procesaron para ese cliente particular a la _instancia coordinadora_.
* `4 = COMMIT` se utiliza solamente por las instancias de _sum_, para que la _instancia coordinadora_ le indique a las demas instancias de _sum_ que envien los datos asociados a ese cliente particular a las instancias de _aggregation_
* `5 = KILL` se utiliza solamente por las instancias de _sum_ consigo mismas, para autoindicarse que deben finalizar el _thread_ utilizado para escuchar por el _exchange_.

> [!NOTE]
> Este protocolo de comunicación no se utiliza entre la instancia de _join_ y _gateway_ porque se deberia cambiar el código del _gateway_, lo cual NO esta permitido.

## Coordinación de instancias

Cada instancia de _sum_ consume mensajes por dos lugares:

* una _queue_ de donde consume los mensajes enviados por la instancia de _gateway_.
* un _exchange_ con una _routing key_ igual a su _id_ de instancia, de donde consume los mensajes enviados por las demas instancias de _sum_ o si misma.

Siempre y cuando se reciban mensajes `DATA`, las instancias de _sum_ pueden operar de forma local e independiente entre si. Sin embargo, cuando una instancia de _sum_ reciba el `EOF`, que denominaremos como _instancia coordinadora_, sera necesario coordinar a las demas instancias de _sum_ para que envien los datos procesados para los cuales se recibio el `EOF`.

> [!WARNING] 
> Basicamente queremos propagar ese `EOF` hacia las demas instancias de _sum_, pero NO es tan sencillo como que la _instancia coordinadora_ lo propage a traves de un _exchange_, porque puede darse el caso de que una o mas instancias de _sum_ esten procesado un dato, les llegue un mensaje `EOF` y despachen los datos hacia las instancias de _aggregation_ antes de que se termine de procesar.

Entonces, el procedimiento a seguir es el siguiente:

> [!NOTE]
> Para el flujo que se va a explicar a continuación se asume que se sabe que todo esto ocurre para una `req_id` en particular, pero no lo menciono en cada paso para no repetir siempre lo mismo.

1. La instancia _sum_ coordinadora envia un mensaje `PREPARE` a las demas instancia de _sum_.

2. Cuando una instancia de _sum_ recibe un mensaje `PREPARE`, responde con un mensaje `OK` solamente a la instancia _sum_ coordinadora.

3. La instancia _sum_ coordinadora espera a recibir `SUM_AMOUNT` mensajes `OK` antes de determinar si todas las instancias de _sum_ ya procesaron los datos, es decir, si la sumatoria de los mensajes procesados por cada uno es igual a los mensajes totales enviados por la instancia de _gateway_.

4. Si se determina que todas las instancias de _sum_ procesaron los datos, entonces la instancia _sum_ coordinadora envia sus datos a las instancias de _aggregation_ y luego envia un mensaje `COMMIT` a las demas instancias de _sum_ para que hagan lo mismo. Entonces, cuando una instancia de _sum_ recibe un mensaje `COMMIT`, envia sus datos a las instancias de _aggregation_.

5. Si se determina que las demas instancias de _sum_ no procesaron todos los datos, simplemente la _instancia coordinadora_ vuelve al paso inicial (1), como si nada hubiera pasado.

Para enviar los datos hacia instancias de _aggregation_ se utiliza un _exchange_, cada instancia de _aggregation_ escucha los mensajes que llegan a ese _exchange_ y tienen como _routing key_ el _id_ de la instancia, que es conocido por las instancias de _sum_.

Entonces para que una instancia de _sum_ determine a que instancia de _aggregation_ debe enviar los mensaje `DATA`, se calcula un _hash_ por cada `req_id`:`fruit` y se aplica el operador `%` para determinar el número de instancia de _aggregation_, que corresponde con el _id_ y que corresponde con la _routing key_ a utilizar.

De esta forma cada mensaje `DATA` va a parar a una instancia de _aggregation_ diferente, ahora cuando una instancia de _sum_ termina de enviar sus datos, como no sabe a que instancias de _aggregation_ los recibieron, publica en el _exchange_ el mensaje `EOF` para que lo reciban todas las instancias de _aggregation_, independientemente de si procesaron o no datos.

Entonces, cada instancia de _aggregation_ cuando reciba un `EOF`, debe verificar si ya recibio `SUM_AMOUNT` mensajes `EOF`.

* Si es asi, entonces procede a enviar los datos a la instancia de _join_.
* Si NO es asi, entonces espera a recibir los `EOF` restantes de las demas instancias de _sum_.

Para enviar los datos hacia la instancia de _join_ se utiliza una única _queue_, entonces cada instancia de _aggregation_ lo que va a hacer es enviar su `TOP_SIZE` local, que se van a ir agrupando en la instancia de _join_ y combinando para determinar el top final. Luego, cuando la instancia de _join_ reciba los `AGGREGATION_AMOUNT` mensajes `EOF`, determina el `TOP_SIZE` definitivo y lo envia por una _queue_ hacia la instancia de _gateway_.

## Escalabilidad
