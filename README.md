# hivejdbc
hivejdbc is `db-api-2.0` compliant **Apache Hive** driver that supports
- kerberos
- ssl
- service discovery via zookeeper
- host-connection list
- and all other jdbc driver options

# installation
```properties
pip3 install hivejdbc
```

## Cursors
`hivejdbc` can use a `dictonary` cursor if desired.

```python
from hivejdbc import connect, DictCursor
conn = connect('example.com', 'default', cursor=DictCursor)

```

## Connection Strings
`hivejdbc` features many `connect` function arguments. Many of these arguments can be ignored 
and are simply present to offer the full options provided by the **Hive** jdbc driver.

To import the `hivejdbc` connect function:
```python
from hivejdbc import connect
```

### Unsecured Hive Instance
to connect to an unsecured hive instance listening on the default port `10000`, and the `default` database:
```python
conn = connect('example.com', 'default')
```

unless all required `hive-jars` are on the classpath already you'll need to define the driver path  
Java uses `jar` files to combine many libraries into one. We'll use our `fatjar` to provide all the required 
dependencies in one place.  
Make sure you're using the correct driver for your **Hive** version.
```python
conn = connect('example.com', 'default', driver='hive-client-hive-2.1.1-hdfs-3.0.3-fatjar.jar')
```

to connect with a custom port of `10015`
```python
conn = connect('example.com', 'default', port=10015)
```

### Username and Password
```python
conn = connect(host='example.com', 
               database='default', 
               port=10015, 
               user='hive_user', 
               password='secret')
```

### SSL
If the hive-server has `ssl` enabled you'll need to provide a `jks` trust store that contains the servers public 
certificate.
```python
conn = connect(host='hive2.example.com',
               port=10015,
               database='default',
               driver='hive-client-hive-2.1.1-hdfs-3.0.3-fatjar.jar',
               ssl=True,
               trust_store='./truststore.jks',
               trust_password='changeit',
               principal='hive/hive2.example.com@EXAMPLE.COM',
               user_principal='hive/hive2.example.com',
               user_keytab='hive.keytab',
               realm='EXAMPLE.COM',
               kdc='kerberosdc.example.com:88')
```

### Kerberos
Authenticating with kerberos can be done a few ways:
1. get valid kerberos credentials via `kinit` before running `hivejdbc`
1. rely on `hivejdbc` to obtain kerberos credentials via a `user-principal` and `user-keytab` provided 
   to the program.


#### Operating System `kinit`
Connect to...
- a `ssl` enabled cluster
- a secured cluster (`kerberos`)
- using the operating systems kerberos configuration
  default locations are searched depending on platform
- using the operating system `kinit` token  
  default locations for the `token-cache` are searched
- if `kinit` has not been performed, or a `token-cache` cannot be found an exception will be thrown
```python
conn = connect(host='hive2.example.com',
               port=10015,
               database='default',
               driver='hive-client-hive-2.1.1-hdfs-3.0.3-fatjar.jar',
               ssl=True,
               trust_store='./truststore.jks',
               trust_password='changeit',
               principal='hive/hive2.example.com@EXAMPLE.COM')
```

#### `hivejdbc` does the `kinit` via `keytab` and a custom `krb5.conf`
connect to... 
- a `ssl` enabled cluster
- a secured cluster (`kerberos`)
- using the operating systems kerberos configuration krb5.conf
- using a `keytab` for authentication  
  the keytab will be used to login via java's built-in kerberos implementation
  avoiding the need for any operating system dependency
- we will provide the `kdc` and `realm` via the `krb5_conf` argument
  if we didn't provide `krb5_conf` argument default locations would be searched within various system paths
```python
conn = connect(host='hive2.example.com',
               port=10015,
               database='default',
               driver='hive-client-hive-2.1.1-hdfs-3.0.3-fatjar.jar',
               ssl=True,
               trust_store='./truststore.jks',
               trust_password='changeit',
               principal='hive/hive2.example.com@EXAMPLE.COM',
               krb5_conf='kerberos/custom_krb5.conf',
               user_principal='hive/hive2.example.com',
               user_keytab='a133041.keytab')
```



#### `hivejdbc` does the `kinit` via `keytab` with no `krb5.conf`
connect to...
- an ssl enabled cluster
- a secured cluster (kerberos)
- not using the operating system or relying on any of its configurations
- manually setting the realm, and the kerberos "kdc" to authenticate to
- using a keytab for authentication
- this configuration is the most portable...
```python
conn = connect(host='hive2.example.com',
               port=10015,
               database='default',
               driver='hive-client-hive-2.1.1-hdfs-3.0.3-fatjar.jar',
               ssl=True,
               trust_store='./truststore.jks',
               trust_password='changeit',
               principal='hive/hive2.example.com@EXAMPLE.COM',
               user_principal='hive/hive2.example.com',
 ~~~~              user_keytab='hive.keytab',
               realm='EXAMPLE.COM',
               kdc='kerberosdc.example.com:88')
```