"""
Test main functionality `hivejdbc`
"""
import unittest
import tempfile
import pytest
import hivejdbc

DOMAIN = 'example.com'
PORT = 10011
DB = 'example'
DEFAULT_PORT = 10000
TRUST_STORE = ''
SERVICE = 'hive'
FILE_TEXT = b'text'


class TestInterface(unittest.TestCase):
    """Test module functionality"""

    def test_import(self):
        # ensure public interface names are presented
        self.assertTrue(hasattr(hivejdbc, 'connect'), 'connect method missing')
        self.assertTrue(callable(hivejdbc.connect))
        self.assertTrue(hasattr(hivejdbc, 'DictCursor'), 'DictCursor missing')


class TestArgumentParser(unittest.TestCase):
    """Test argument passing"""

    def test_minimal_mandatory(self):
        with pytest.raises(ValueError):
            hivejdbc.HiveArgParser(host=DOMAIN, port=PORT).parse()  # missing database
        with pytest.raises(ValueError):
            hivejdbc.HiveArgParser(database=DB, port=PORT).parse()  # missing host

    def test_minimal_defaults(self):
        parser = hivejdbc.HiveArgParser(DOMAIN, DB)
        args = parser.parse()
        self.assertEqual(args.get('host'), DOMAIN)
        self.assertEqual(args.get('port'), DEFAULT_PORT)
        self.assertEqual(args.get('database'), DB)
        self.assertEqual(args.get('driver'), None)
        self.assertDictEqual(args.get('properties'), {})

    def test_named_defaults(self):
        parser = hivejdbc.HiveArgParser(host=DOMAIN, database=DB, port=PORT)
        args = parser.parse()
        self.assertEqual(args.get('host'), DOMAIN)
        self.assertEqual(args.get('port'), PORT)
        self.assertEqual(args.get('database'), DB)
        self.assertEqual(args.get('driver'), None)

    def test_ssl(self):
        # ssl by itself
        # ssl + trust_store + password
        # errors on trust_store by itself, ssl + trust_store

        # test that 'ssl' can be set by itself
        parser = hivejdbc.HiveArgParser(host=DOMAIN, database=DB, port=PORT, ssl=True)
        args = parser.parse()
        self.assertTrue(args.get('ssl'))

        # ensure arguments are not allowed without `ssl` being set
        for arg in ['trust_store', 'trust_password']:
            with pytest.raises(ValueError):
                kwargs = {'host': DOMAIN, 'database': DB}
                kwargs[arg] = 'string'
                hivejdbc.HiveArgParser(**kwargs).parse()

    def test_trust_store(self):
        pass

    def test_kerberos_minimal(self):
        # test kerberos related args
        parser = hivejdbc.HiveArgParser(host=DOMAIN, database=DB, port=PORT, principal=SERVICE)
        args = parser.parse()
        self.assertEqual(args.get('principal'), SERVICE)

    def test_kerberos_krb5(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(FILE_TEXT)
            temp.flush()

            parser = hivejdbc.HiveArgParser(host=DOMAIN,
                                            database=DB,
                                            port=PORT,
                                            principal=SERVICE,
                                            krb5_conf=temp.name)
            args = parser.parse()
            self.assertEqual(args.get('principal'), SERVICE)
            self.assertEqual(args.get('krb5_conf'), temp.name)

    def test_kerberos_keytab(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(FILE_TEXT)
            temp.flush()

            parser = hivejdbc.HiveArgParser(host=DOMAIN,
                                            database=DB,
                                            port=PORT,
                                            principal=SERVICE,
                                            krb5_conf=temp.name,
                                            user_principal='user@EXAMPLE.COM',
                                            user_keytab=temp.name)
            args = parser.parse()
            self.assertEqual(args.get('principal'), SERVICE)
            self.assertEqual(args.get('krb5_conf'), temp.name)
            self.assertEqual(args.get('user_principal'), 'user@EXAMPLE.COM')
            self.assertEqual(args.get('user_keytab'), temp.name)

    def test_kerberos_keytab_adv(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(FILE_TEXT)
            temp.flush()

            parser = hivejdbc.HiveArgParser(host=DOMAIN,
                                            database=DB,
                                            port=PORT,
                                            principal=SERVICE,
                                            user_principal='user@EXAMPLE.COM',
                                            user_keytab=temp.name,
                                            kdc='example.com:88',
                                            realm='EXAMPLE.COM')
            args = parser.parse()
            self.assertEqual(args.get('principal'), SERVICE)
            self.assertEqual(args.get('user_principal'), 'user@EXAMPLE.COM')
            self.assertEqual(args.get('user_keytab'), temp.name)
            self.assertEqual(args.get('kdc'), 'example.com:88')
            self.assertEqual(args.get('realm'), 'EXAMPLE.COM')

    def test_all(self):
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(FILE_TEXT)
            temp.flush()

            parser = hivejdbc.HiveArgParser(host=DOMAIN,
                                            database=DB,
                                            port=PORT,
                                            principal=SERVICE,
                                            user_principal='user@EXAMPLE.COM',
                                            user_keytab=temp.name,
                                            kdc='example.com:88',
                                            realm='EXAMPLE.COM',
                                            ssl=True,
                                            trust_store=temp.name,
                                            trust_password='secret',
                                            properties={'a': 1},
                                            transport='binary',
                                            init_file='script.hsql',
                                            service_discovery_mode='zooKeeper',
                                            zookeeper_namespace='hive')
            args = parser.parse()
            self.assertEqual(args.get('principal'), SERVICE)
            self.assertEqual(args.get('user_principal'), 'user@EXAMPLE.COM')
            self.assertEqual(args.get('user_keytab'), temp.name)
            self.assertEqual(args.get('kdc'), 'example.com:88')
            self.assertEqual(args.get('realm'), 'EXAMPLE.COM')
            self.assertEqual(args.get('ssl'), True)
            self.assertEqual(args.get('trust_store'), temp.name)
            self.assertEqual(args.get('trust_password'), 'secret')
            self.assertDictEqual(args.get('properties'), {'a': 1})
            self.assertEqual(args.get('transport'), 'binary')
            self.assertEqual(args.get('init_file'), 'script.hsql')
            self.assertEqual(args.get('service_discovery_mode'), 'zooKeeper')
            self.assertEqual(args.get('zookeeper_namespace'), 'hive')
