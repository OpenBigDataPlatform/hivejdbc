__all__ = ['connect', 'DictCursor', 'apilevel', 'threadsafety', 'paramstyle']

import logging
import socket
import ipaddress
from os.path import abspath, isfile
import getpass

from jpype import JClass

from pyjdbc.connect import ArgumentParser, ArgumentOpts, ConnectFunction, ConnectArguments, Decorator
from pyjdbc.java import Properties, Jvm, System
from pyjdbc.dbapi import JdbcConnection, JdbcCursor, JdbcDictCursor
from pyjdbc.exceptions import Error
from pyjdbc import kerberos

from hivejdbc.types import HiveTypeConversion

apilevel = '2.0'
threadsafety = 1
paramstyle = 'named'

DRIVER_CLASS = 'org.apache.hive.jdbc.HiveDriver'

# errors:
# java.lang.RuntimeException: java.lang.RuntimeException: Illegal Hadoop Version: Unknown (expected A.B.* format)
#    see: https://community.cloudera.com/t5/Community-Articles/Connecting-DbVisualizer-and-DataGrip-to-Hive-with-Kerberos/ta-p/248539
#    see: https://github.com/timveil/hive-jdbc-uber-jar/blob/master/src/main/java/org/apache/hadoop/util/VersionInfo.java
# java.security.UnrecoverableKeyException: java.security.UnrecoverableKeyException: Password verification failed
# Caused by: org.ietf.jgss.GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt
# javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
# Caused by: org.ietf.jgss.GSSException: No valid credentials provided (Mechanism level: Attempt to obtain new INITIATE credentials failed! (null))
# -- when a ticket cannot be found.
# org.apache.thrift.transport.TTransportException: org.apache.thrift.transport.TTransportException: javax.net.ssl.SSLHandshakeException: Remote host terminated the handshake
# -- when trust-store is bad

# org.apache.hive.service.cli.HiveSQLException: org.apache.hive.service.cli.HiveSQLException:
# Error while compiling statement: FAILED: SemanticException [Error 10293]:
# Unable to create temp file for insert values Expression of type TOK_FUNCTION not supported in insert/values
# -- when user attempts to directly insert an array value


class HiveArgParser(ArgumentParser):
    host = ArgumentOpts(position=0, argtype=str, description='Hive Host, ie: `example.org`, can also be a comma'
                                                             'separated list of hosts to attempt')
    database = ArgumentOpts(position=1, argtype=str, description='Database name to connect to, `default`')
    port = ArgumentOpts(argtype=int, default=10000, description='Hive port, deafults to `10000`')
    driver = ArgumentOpts(argtype=str, description='Location to hive uber-jar')
    cursor = ArgumentOpts(argtype=JdbcCursor, description='cursor class for queries')
    ssl = ArgumentOpts(argtype=bool, description='enable ssl connection mode, if the server is running with '
                                                 'ssl certificates enabled this is required')
    trust_password = ArgumentOpts(argtype=str, secret=True, requires=['trust_store', 'ssl'])
    user = ArgumentOpts(argtype=str, description='Hive username if using username/password auth')
    password = ArgumentOpts(argtype=str, secret=True, requires=['user'], description='Hive basic auth password')
    user_principal = ArgumentOpts(argtype=str, description='Kerberos user principal', requires=['user_keytab'])
    realm = ArgumentOpts(argtype=str, description='Kerberos realm (domain), if set, "realm" must also be set,'
                                                  'normally this value can be obtained automatically '
                                                  'from "default_realm" within krb5.conf',
                         requires=['principal', 'user_keytab', 'kdc'])
    properties = ArgumentOpts(argtype=dict, default={},
                              description='properties passed to org.apache.hive.jdbc.HiveDriver "connect" method')
    transport = ArgumentOpts(argtype=str, default='binary', choices=('binary', 'http'))
    http_path = ArgumentOpts(argtype=str, description='HTTP endpoint for when HiveServer2 is running HTTP mode.\n'
                                                      'this is a rarely used option. Only set this if `transport` '
                                                      'is set to `binary`')
    init_file = ArgumentOpts(argtype=str, description='This script file is written with SQL statements which will be '
                                                      'executed automatically after connection')
    service_discovery_mode = ArgumentOpts(argtype=str, choices=['zooKeeper'], requires=['zookeeper_namespace'],
                                          description='If using zookeeper service discovery you must set this')
    zookeeper_namespace = ArgumentOpts(argtype=str,
                                       requires=['service_discovery_mode'],
                                       description='Zookeeper namespace string for service discovery')

    @Decorator.argument(argtype=str, requires=['trust_password', 'ssl'])
    def trust_store(self, path):
        """Path to the java ssl trust-store, generally required if ssl=True"""
        if not isinstance(path, str):
            raise ValueError('expected `str`, got: {}'.format(type(path)))

        if not isfile(path):
            raise ValueError('not a valid file')
        return path

    @Decorator.argument(argtype=str, excludes=['username', 'password'])
    def principal(self, user):
        """Hive SERVICE principal, usually "hive" - should be fully qualified: `hive@EXAMPLE.COM`"""
        if not isinstance(user, str):
            raise ValueError('expected `str`, got: {}'.format(type(path)))

        if not Jvm.is_running():
            Jvm.add_argument('javax.security.auth.useSubjectCredsOnly',
                             '-Djavax.security.auth.useSubjectCredsOnly=false')
        else:
            System.set_property('javax.security.auth.useSubjectCredsOnly', 'false')
        return user

    @Decorator.argument(argtype=str, requires=['principal', 'user_principal'])
    def user_keytab(self, path):
        """Kerberos keytab - if provided the module will attempt kerberos login without the need for ``kinit``"""
        if not isinstance(path, str):
            raise ValueError('expected `str`, got: {}'.format(type(path)))

        if not isfile(path):
            raise ValueError('not a valid file')
        return path

    @Decorator.argument(argtype=str, requires=['principal'])
    def krb5_conf(self, path):
        """Kerberos krb5.conf - default locations for the file are platform dependent
         or set via environment variable: "KRB5_CONFIG" - if your configuration is in a default location you
         typically do not need to explicitly provide this configuration."""
        if not isinstance(path, str):
            raise ValueError('expected `str`, got: {}'.format(type(path)))

        if not Jvm.is_running():
            Jvm.add_argument('java.security.krb5.conf', '-Djava.security.krb5.conf={}'.format(path))
        else:
            System.set_property('java.security.krb5.conf', path)

        if not isfile(path):
            raise ValueError('not a valid file')
        return path

    @Decorator.argument(argtype=str, requires=['principal', 'user_principal', 'user_keytab'])
    def kdc(self, kdc_host):
        """Kerberos kdc hostname:port combination"""
        if not isinstance(kdc_host, str):
            raise ValueError('expecting `str`, got: {}'.format(type(kdc_host)))

        if ':' not in kdc_host or len(kdc_host.split(':')) != 2 or not str(kdc_host.split(':')[-1]).isdigit():
            raise ValueError('kdc must contain a host and numerical port separated by ":", '
                             'kdc invalid: {}'. format(kdc_host))

        if not Jvm.is_running():
            Jvm.add_argument('java.security.krb5.kdc', '-Djava.security.krb5.kdc={}'.format(kdc_host))
        else:
            System.set_property('java.security.krb5.kdc', kdc_host)

        return kdc_host

    @Decorator.argument(argtype=dict)
    def hive_conf_list(self, conf_map):
        """
        dictionary of key/value pairs of hive configuration variables for the session.
        the driver will automatically url encode the variables as needed
        """
        raise NotImplementedError('hive_conf_list is not yet supported')

    @Decorator.argument(argtype=dict)
    def hive_var_list(self, var_map):
        """
        dictionary of key/value pairs of Hive variables for this session.
        the driver will automatically url encode the variables as needed
        """
        raise NotImplementedError('hive_var_list is not yet supported')


def check_server(hostname, port):

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        try:
            address = socket.gethostbyname(hostname.strip())
        except socket.gaierror as e:
            raise Error('Hive server at "{}:{}" is not reachable - {}'.format(hostname, port, e))
    else:
        address = hostname

    try:
        s.connect((address, int(port)))
        s.shutdown(2)
    except Exception as e:
        raise Error('No Hive server is listening at "{}:{}" - {}'.format(hostname, port, e))


class HiveConnect(ConnectFunction):

    def handle_args(self, args: ConnectArguments):
        """
        Handle args is called before the JVM is started

        :param args:
        :return:
        """
        # set driver path from connect function argument `driver`
        self.driver_path = abspath(args.get('driver'))

        # override the cursor class if requested.
        if args.get('cursor'):
            self.cursor_class = args.get('cursor')

        # handle various ways kerberos can be configured:
        if args.get('principal'):  # kerberos is method of auth
            if args.get('user_keytab'):
                # if user_keytab is set, this means we are expected to perform the kerberos authentication.
                # we'll use jaas to accomplish this.
                kerberos.configure_jaas(use_password=False, no_prompt=True, use_ticket_cache=False,
                                        principal=args.user_principal, keytab=args.user_keytab)
            elif args.get('principal'):
                # If principal is set, but user_keytab is not set, this means we're just looking for an existing
                # kinit session to authenticate
                # this jaas configuration DOES NOT PROMPT for username/password
                # but looks for an existing kerberos ticket created by the operating system or `kinit`
                kerberos.configure_jaas(use_password=False, no_prompt=True, use_ticket_cache=True)
            else:
                pass

        if not Jvm.is_running():
            # we don't want to see warnings about log4j not being configured, so we'll disable the log4j logger
            # this will not prevent other log messages from appearing in stdout
            Jvm.add_argument('org.apache.logging.log4j.simplelog.StatusLogger.level',
                             '-Dorg.apache.logging.log4j.simplelog.StatusLogger.level=OFF')
        else:
            System.set_property('org.apache.logging.log4j.simplelog.StatusLogger.level', 'OFF')

        # if the realm is not set try to set it from the principal
        if args.get('kdc') and not args.get('realm'):
            args.realm = (kerberos.realm_from_principal(args.principal) or
                          kerberos.realm_from_principal(args.get('user_principal', '')))
            if not args.realm:
                raise ValueError('Argument "realm" must be set if "kdc" is set, either explicitly or in '
                                 'the principal name')

        if args.get('kdc') and args.get('realm'):
            # set the realm

            if not Jvm.is_running():
                Jvm.add_argument('java.security.krb5.realm', '-Djava.security.krb5.realm={}'.format(args.realm))
            else:
                System.set_property('java.security.krb5.realm', args.realm)

    def get_connection(self, driver_class: JClass, args: ConnectArguments):
        """
        Hive specific implementation of JdbcConnection setup

        When this method is called the jvm has been started, and the driver_class has been found

        see: https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients

        :param driver_class: HiveDriver `JClass` reference
        :type driver_class: org.apache.hive.jdbc.HiveDriver
        :param args: Connection arguments containing options derived from ``hivejdbc.HiveArgParser``
        :type args: pyjdbc.connect.ConnectArguments
        :return: db-api-2 connection instance
        :rtype: pyjdbc.dbapi.JdbcConnection
        """
        log = logging.getLogger(self.__class__.__name__)

        if ',' not in args.host:
            check_server(args.host, args.port)

        HiveDriver = driver_class

        java_props = Properties.from_dict(args.properties or {})

        options = []

        # Create the Connection String based on Arguments
        host_part = 'jdbc:hive2://{host}:{port}/{database}'.format(host=args.host,
                                                                   port=args.port,
                                                                   database=args.database)
        options.append(host_part)

        # -- all options after the database are called "session-variables" ------------------------

        # Configure initFile - must be the first option in session-vars
        if args.get('init_file'):
            options.append('initFile={}'.format(args.init_file))

        # username/password support - note that user can be given without password for unsecured hive servers
        if args.get('user'):
            options.append('user={}'.format(args.user))
        if args.get('password'):
            options.append('password={}'.format(args.password))

        # Configure transport mode
        if args.get('transport'):
            options.append('transportMode={}'.format(args.transport))

        # Configure SSL options
        if args.get('ssl'):
            options.append('ssl=true')
        if args.get('trust_store'):
            options.append('sslTrustStore={}'.format(args.trust_store))
        if args.get('trust_password'):
            options.append('trustStorePassword={}'.format(args.trust_password))

        # Configure Kerberos if given
        if args.get('principal'):
            options.append('principal={}'.format(args.principal))

        if args.get('transport') == 'http' and args.get('http_path'):
            options.append('httpPath={}'.format(args.http_path))

        if args.get('service_discovery_mode'):
            options.append('serviceDiscoveryMode={}'.format(args.service_discovery_mode))
            options.append('zooKeeperNamespace={}'.format(args.zookeeper_namespace))

        conn_str = ';'.join(options)

        #if args.get('user_keytab'):
        #    self.kerberos_login(args)

        log.debug('hive connection string: %s', conn_str)  # TODO make secure

        hive_driver = HiveDriver()
        try:
            # 	connect(String url, Properties info)
            java_conn = hive_driver.connect(conn_str, java_props)
        except JClass('java.sql.SQLException') as e:
            # TODO self.handle_exception(e)
            raise

        return JdbcConnection(connection=java_conn,
                              cursor_class=self.cursor_class,
                              type_conversion=self.type_conversion())

    def handle_exception(self, exc):
        """
        Looks for known exceptions and raises more useful errors.

        :param exc:
        :return:
        """
        # TODO handle known exceptions
        # resolve the exception hierarchy so we can search through all exception messages.
        hierarchy = []
        cause = exc
        while True:
            cause = getattr(cause, '__cause__', None)
            if not cause:
                break
            else:
                hierarchy.append(cause)

    def handle_missing_kerberos_ticket(self, exc):
        pass


class DictCursor(JdbcDictCursor):
    pass


connect = HiveConnect(driver_path=None,  # we'll set this later based on user input
                      driver_class=DRIVER_CLASS,
                      parser=HiveArgParser,
                      type_conversion=HiveTypeConversion,
                      runtime_invocation_ok=False)