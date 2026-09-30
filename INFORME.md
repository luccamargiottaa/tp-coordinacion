## Coordinación entre nodos

En cada etapa del *pipeline* de la arquitectura es importante coordinar el envío de datos hacia el siguiente paso. Esto se realiza mediante el envío de un *EOF message*.

El **Gateway** envía este mensaje una vez que terminó de enviar todos los mensajes del cliente correspondiente. Este mensaje cuenta con la cantidad de registros de ese cliente para permitir a los **Sums** la coordinación del envío de sus propios *EOF*. Al ser enviado por una cola, el mensaje le llegará a un solo **Sum**, por lo que fue necesario comunicar a los **Sums** entre sí.

Para ello se decidió conectar a los **Sums** a un *exchange* mediante el cual publican y reciben mensajes de coordinación. Estos mensajes incluyen:

* *EOF message*: es el mensaje enviado por el **Gateway** que un **Sum** recibe por la cola de *input*. Es reenviado por el *exchange* para que todos los **Sums** sepan que se enviaron todos los registros del cliente y la cantidad total de estos.

* *Record Amount message*: cuenta con la cantidad de registros procesados por un **Sum** en específico. Esta cantidad es sumada con la del resto de mensajes hasta que se alcanza la cantidad total de registros del cliente.

Cuando la cantidad de registros procesados equivale a la cantidad total de registros del cliente, cada **Sum** realiza el envío de sus datos hacia los **Aggregation** seguido de un *EOF message*, el cual es *broadcasteado* a todos los **Aggregation**. Por lo tanto, cada **Aggregation** debe esperar a recibir por el *exchange* cada uno de los *EOF* enviados por los **Sums**.

Luego, al terminar su procesamiento y envío del *top* parcial, cada **Aggregation** envía un *EOF message* al **Join**, el cual, al recibir todos los *EOFs* enviados, envía el *top* final al **Gateway**. Finalmente, este *top* final es devuelto al cliente.

## Escalabilidad del sistema

Es de suma importancia que el sistema esté preparado para escalar con respecto a la cantidad de clientes y los volúmenes de datos.

Para manejar grandes cantidades de clientes, fue necesario incluir un `ClientId` en todos los mensajes internos del sistema. Esto permitió distinguir entre registros de los distintos clientes y así procesar varios clientes en simultáneo.

Para procesar grandes volúmenes de datos es necesario utilizar las máximas capacidades del sistema. Es decir, se necesita una distribución equitativa del procesamiento realizado por los distintos controles.

El uso de las colas provistas por el *middleware* utilizado permite contar con *fairness* en el envío de registros desde el **Gateway** hacia los **Sums**. Los **Sums** no distinguen entre registros y son capaces de procesar registros de todos los clientes.

Al ser enviados por un *exchange*, los registros enviados de los **Sums** a los **Aggregation** deben ser distinguidos por algún criterio para evitar procesamiento duplicado. Este criterio se basa en un *hash* del nombre de la fruta y el `ClientId`, con el cual se decide a qué **Aggregation** el registro debe ser enviado. Un *hash* del nombre entero de la fruta en un principio provee una distribución equitativa. Además, permite mantener la integridad del *top* final al no haber colisiones entre los distintos *tops* parciales.

Sin embargo, existe el caso en el que una fruta es mucho más popular que el resto entre clientes, lo que causaría que un **Aggregation** deba realizar mucho más procesamiento que el resto. Este problema se soluciona agregando al *hash* el `ClientId`.