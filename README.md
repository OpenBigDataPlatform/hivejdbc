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

### Cursors support `with`
```python
from hivejdbc import connect
conn = connect('example.com', database='default')
with conn.cursor() as cursor:
    cursor.execute('select * from test.persons')
    rows = cursor.fetchall()
```

### Cursors are iterable
```python
from hivejdbc import connect
conn = connect('example.com', database='default')
cursor = conn.cursor()
cursor.execute('select * from test.persons')
for row in cursor:
    print(row[0])
cursor.close()
```

### Cursors Support
- `fetchone()`
- `fetchmany()`
- `fetchall()`

```python
from hivejdbc import connect
conn = connect('example.com', database='default')
cursor = conn.cursor()
cursor.execute('select * from test.persons')
cursor.fetchone() # fetch first row or None
cursor.fetchmany(5) # fetch next 5 rows
cursor.fetchall() # fetch remaining rows or empty list
cursor.close()
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
               user_keytab='user.keytab')
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
               user_keytab='hive.keytab',
               realm='EXAMPLE.COM',
               kdc='kerberosdc.example.com:88')
```

## Queries and Parameters

For these examples we'll setup a `test` database with a `persons` table...

```python
cursor = conn.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS test')
cursor.execute('DROP TABLE IF EXISTS test.persons')
cursor.execute('CREATE TABLE test.persons (name VARCHAR(64), age INT, address STRING, '
               'first TIMESTAMP, balance DECIMAL(12,2))')
```

Our table sql will have 5 columns defined in the above statement:
```sql
CREATE TABLE test.persons (
    name VARCHAR(64), 
    age INT, 
    address STRING,
    first TIMESTAMP, 
    balance DECIMAL(12,2)
)
```

### single insert
Let's insert a single record:
```python
cursor.execute('''    
INSERT INTO TABLE test.persons (name, age, address, first, balance)
VALUES ('john doe', 35, '1583 Whistling Pines Dr, Redstone CO 80612', '08-22-1981 00:00:00', '100.10')
''')
```

### `positional` parameterized sql query
Insert a single record, using paramterized arguments that will automatically be escaped.    
This prevents sql injection as well

```python
cursor.execute('''    
INSERT INTO TABLE test.persons (name, age, address, first, balance)
VALUES (%s, %s, %s, %s, %s)
''', ['Kevin Jones', 28, '802 1st st, Raleigh NC', '12-23-2020 00:00:00', 85.25])
```

The signature of `execute` is:
```python
def execute(sql, params=None):
    ""
```
- **sql** is the sql statement
- **params** are `named (dict)` or `positional (sequence)` arguments used by the sql statement for variable 
  substitution

### `named` parameterized sql query
**INSERT** with named parameters 

In addition to positional parameters using `%s` we support `named parameters` as well.
 
You can see the named arguments are defined below in the `sql` statement as: `(:name, :age, :addr, :dt, :bal)`  

The second parameter to the `execute` method is a `dictionary` where the keys are equal to the parameters defined in the sql
```python
cursor.execute('''
INSERT INTO TABLE test.persons (name, age, address, first, balance)
VALUES (:name, :age, :addr, :dt, :bal)
''', {'name': 'Bob Clark',
      'age': 41,
      'addr': '348 W Dickinson Rd, Norfolk VA',
      'dt': '12-23-2020 00:00:00',
      'bal': 200.20})
```

### Using `executemany`
You can execute many queries in one python statement using `executemany`  

Note that this is for programmer ease of use; hive's `jdbc` driver does not support `batch-mode`, so this functionality is faked and is no more 
efficient than executing 3 statements individually.
```
cursor.executemany('''
INSERT INTO TABLE test.persons (name, age, address, first, balance)
VALUES (%s, %s, %s, %s, %s)
''', [
    ('john doe', 35, '1583 Whistling Pines Dr, Redstone CO 80612', '08-22-1981 00:00:00', 100.10),
    ('Kevin Jones', 28, '802 1st st, Raleigh NC', '12-23-2020 00:00:00', 85.25),
    ('Bob Clark', 41, '348 W Dickinson Rd, Norfolk VA', '12-23-2020 00:00:00', 200.20)
])
```
