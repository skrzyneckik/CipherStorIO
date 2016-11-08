# CipherStoreIO
use SqlCipher with StoreIO

StoreIO is awsome ORM for Android and can be found here:
https://github.com/pushtorefresh/storio

SQLCipher is database ciphering lib. It can be found here:
https://github.com/sqlcipher/android-database-sqlcipher

This project makes using StorIO with sqlCipher easy

How to get it
--
Add jitpack to your build.gradle
```groovy
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}
```
add dependency
```groovy
compile 'com.github.pbochenski:CipherStoreIO:master-SNAPSHOT'
```
release build comming soon

How to use it
--
* Initialize sqlCipher in your Application onCreate
```kotlin
SQLiteDatabase.loadLibs(this)
```

* create dbhelper
```java
import net.sqlcipher.database.SQLiteDatabase;
import net.sqlcipher.database.SQLiteOpenHelper;

class DBHelper extends SQLiteOpenHelper {

    public DBHelper(Context context) {
        super(context, "db", null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        sqLiteDatabase.execSQL("some table creating sql code here");
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        //update your database here
    }
}
```

* initialize StorIO
```java
import pl.orendi.cipherstoreio.CipherStoreIO
...
CipherStoreIO.builder()
                .sqliteOpenHelper(DBHelper(this), "password")
                .build()
```

* use StorIO like a boss
