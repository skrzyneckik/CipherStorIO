package com.siili.storeiowithsqlcipher

import android.content.ContentValues
import android.content.Context
import android.os.Bundle
import android.support.v7.app.AppCompatActivity
import android.util.Log
import com.facebook.stetho.Stetho
import com.pushtorefresh.storio.sqlite.operations.put.DefaultPutResolver
import com.pushtorefresh.storio.sqlite.queries.InsertQuery
import com.pushtorefresh.storio.sqlite.queries.Query
import com.pushtorefresh.storio.sqlite.queries.UpdateQuery

import net.sqlcipher.database.SQLiteDatabase
import net.sqlcipher.database.SQLiteOpenHelper
import pl.orendi.cipherstoreio.CipherStoreIO

class MainActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Stetho.initializeWithDefaults(this)
        setContentView(R.layout.activity_main)

        SQLiteDatabase.loadLibs(this)

        val base = CipherStoreIO.builder()
                .sqliteOpenHelper(DBHelper(this), "password")
                .build()

        base.put()
                .contentValues(ContentValues().apply {
                    put(UsersTable.COLUMN_ID, 1)
                    put(UsersTable.COLUMN_NAME, "dupa")
                })
                .withPutResolver(UsersTable.putResolver)
                .prepare().executeAsBlocking()

        val cursor = base.get()
                .cursor()
                .withQuery(Query.builder().table(UsersTable.TABLE).build())
                .prepare()
                .executeAsBlocking()

        if (cursor.moveToFirst()) {
            do {
                Log.d("BOCHEN", "${cursor.getInt(cursor.getColumnIndex(UsersTable.COLUMN_ID))} = ${cursor.getString(cursor.getColumnIndex(UsersTable.COLUMN_NAME))}")
            } while (cursor.moveToNext())
        }
        cursor.close()
    }
}

class DBHelper(context: Context) : SQLiteOpenHelper(context, "db", null, 1) {
    override fun onCreate(db: SQLiteDatabase?) {
        db?.execSQL(UsersTable.CREATE_TABLE_QUERY)
    }

    override fun onUpgrade(p0: SQLiteDatabase?, p1: Int, p2: Int) {
        //no upgrade in this example
    }

}

object UsersTable {
    val TABLE = "users"
    val COLUMN_ID = "_id"
    val COLUMN_NAME = "name"

    val CREATE_TABLE_QUERY = "CREATE TABLE $TABLE (" +
            "$COLUMN_ID INTEGER NOT NULL PRIMARY KEY," +
            "$COLUMN_NAME TEXT NOT NULL" +
            ");"

    val putResolver = object : DefaultPutResolver<ContentValues>() {
        override fun mapToUpdateQuery(values: ContentValues): UpdateQuery {
            return UpdateQuery.builder().table(TABLE).where("$COLUMN_ID = ?").whereArgs(values.getAsString(COLUMN_ID)).build()
        }

        override fun mapToContentValues(values: ContentValues): ContentValues {
            return values
        }

        override fun mapToInsertQuery(`object`: ContentValues): InsertQuery {
            return InsertQuery.builder().table(TABLE).build()
        }

    }
}
