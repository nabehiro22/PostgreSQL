using Npgsql;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace PostgreSQL
{
	class Program
	{
		static void Main(string[] args)
		{
			/***** 通常の接続 *****/
			//open1();

			/***** NpgsqlConnectionにawait usingを使う *****/
			//Task t2 = open2();
			//t2.Wait();

			/***** CancellationTokenSourceなしの非同期接続 *****/
			//Task t3 = open3();
			//t3.Wait();

			/***** CancellationTokenSourceありの非同期接続 *****/
			//using CancellationTokenSource tokenSource = new();
			//Task t4 = open4(tokenSource);
			//t4.Wait();

			/***** NpgsqlCommandで接続 *****/
			//open5();

			/***** 基本的なテーブルの作成 *****/
			//newTable1();

			/***** テーブルが存在してなければテーブルの作成 *****/
			//newTable2();

			/***** テーブルが存在してない時だけ作成 *****/
			//newTable2();

			/***** LISTパーティションのあるテーブル作成 *****/
			//listTable();
			// メインテーブル下にパーティションを作成
			//listPartition("numeric_a");
			// データを追加
			//listData();

			/***** RANGEパーティションがあるテーブル作成 *****/
			//rangeTable();
			// メインテーブル下にパーティションを作成
			//rangePartition("range_numeric");
			// データを追加
			//rangeData();

			/***** 日付情報の年だけを見るLISTパーティションがあるテーブルの作成 *****/
			listDateTable();
			// メインテーブル下にパーティションを作成
			listDatePartition();
			// データを追加
			listDateData();

			// テーブルの削除
			dropTable();

		}

		#region 接続
		/// <summary>
		/// 標準的な使い方
		/// </summary>
		static void open1()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
		}

		/// <summary>
		/// NpgsqlConnectionの破棄を非同期に実行する使い方
		/// </summary>
		/// <returns></returns>
		static async Task open2()
		{
			await using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
		}

		/// <summary>
		/// CancellationTokenなしの非同期接続
		/// </summary>
		/// <returns></returns>
		static async Task open3()
		{
			await using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			await con.OpenAsync();
		}

		/// <summary>
		/// CancellationTokenありの非同期接続
		/// </summary>
		/// <param name="tokenSource"></param>
		/// <returns></returns>
		static async Task open4(CancellationTokenSource tokenSource)
		{
			await using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			// 引数にCancellationTokenSourceを使用する場合はtokenSource.Cancel();で動作をキャンセルさせる
			try
			{
				// 例は接続する前にCancelメソッドを実行し例外「OperationCanceledException」に来るかの実験
				tokenSource.Cancel();
				await con.OpenAsync(tokenSource.Token);
			}
			catch (OperationCanceledException)
			{

			}
		}

		/// <summary>
		/// NpgsqlCommandで接続
		/// </summary>
		static void open5()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			using NpgsqlCommand cmd = new NpgsqlCommand();
			cmd.Connection = con;
			cmd.Connection.Open();
			cmd.Connection.Close();
		}
		#endregion

		/// <summary>
		/// テーブルを削除
		/// </summary>
		static void dropTable()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@"DROP TABLE ""data"";", con);
			try
			{
				_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				// 存在しないテーブルを削除すると例外
				string msg = exc.Message;
			}
		}

		#region テーブルの作成
		/// <summary>
		/// テーブル作成
		/// </summary>
		static void newTable1()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@"CREATE TABLE ""data""(id serial PRIMARY KEY, time timestamp DEFAULT clock_timestamp(), name text, numeric integer)", con);
			try
			{
				_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException)
			{

			}
		}

		/// <summary>
		/// テーブルが存在してない時だけ作成
		/// </summary>
		static void newTable2()
		{

			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@"CREATE TABLE IF NOT EXISTS ""data""(id serial PRIMARY KEY, time timestamp DEFAULT clock_timestamp(), name text, numeric integer)", con);
			try
			{
				_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException)
			{

			}
		}

		#region LISTパーティション
		/// <summary>
		/// LISTパーティションを持ったテーブル
		/// </summary>
		static void listTable()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@"CREATE TABLE ""data""(time timestamp DEFAULT clock_timestamp(), name text, numeric integer) PARTITION BY LIST (name)", con);
			try
			{
				_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				string msg = exc.Message;
			}
		}

		/// <summary>
		/// LISTパーテイションテーブル作成
		/// テーブルの文字列でパーティション作成
		/// </summary>
		static void listPartition(string PartitionNname)
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@$"CREATE TABLE {PartitionNname} PARTITION OF ""data"" FOR VALUES IN ('a');", con);
			_ = cmd.ExecuteNonQuery();
			// IF NOT EXISTSがあればパーティションテーブルの重複生成も抑制できる
			cmd.CommandText = @$"CREATE TABLE IF NOT EXISTS {PartitionNname} PARTITION OF ""data"" FOR VALUES IN ('a');";
			_ = cmd.ExecuteNonQuery();
			// パーティションテーブルが存在しない値を入れるパーティションテーブル
			cmd.CommandText = @$"CREATE TABLE default_name PARTITION OF ""data"" DEFAULT;";
			_ = cmd.ExecuteNonQuery();
		}

		/// <summary>
		/// データを追加
		/// </summary>
		static void listData()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new();
			cmd.Connection = con;
			try
			{
				cmd.CommandText = @"INSERT INTO ""data""(name, numeric) VALUES ('a', 1);";
				var result1 = cmd.ExecuteNonQuery();
				cmd.CommandText = @"INSERT INTO ""data""(name, numeric) VALUES ('b', 2);";
				var result2 = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				// デフォルトパーティションテーブルが存在してないとパーティションテーブルの条件に一致しないINSERTがあると例外が発生
				// "23514: no partition of relation \"data\" found for row"
				string msg = exc.Message;
			}
		}
		#endregion

		#region RANGEパーティション
		/// <summary>
		/// RANGEパーティションを持ったテーブル
		/// </summary>
		static void rangeTable()
		{

			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@"CREATE TABLE ""data""(time timestamp DEFAULT clock_timestamp(), name text, numeric integer) PARTITION BY RANGE (numeric)", con);
			try
			{
				_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				string msg = exc.Message;
			}
		}

		/// <summary>
		/// RANGEパーテイションテーブル作成
		/// テーブルの文字列でパーティション作成
		/// </summary>
		static void rangePartition(string PartitionNname)
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			try
			{
				// 範囲は 下限値 <= データ < 上限値となる 
				using NpgsqlCommand cmd = new(@$"CREATE TABLE {PartitionNname}_1 PARTITION OF ""data"" FOR VALUES FROM (1) TO (6);", con);
				_ = cmd.ExecuteNonQuery();
				cmd.CommandText = @$"CREATE TABLE {PartitionNname}_2 PARTITION OF ""data"" FOR VALUES FROM (6) TO (11);";
				_ = cmd.ExecuteNonQuery();
				// パーティションテーブルが存在しない値を入れるパーティションテーブル
				//cmd.CommandText = @$"CREATE TABLE default_name PARTITION OF ""data"" DEFAULT;";
				//_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				// パーティションテーブルの範囲が重複すると例外が発生
				// "42P17: partition \"range_data_2\" would overlap partition \"range_data_1\""
				string msg = exc.Message;
			}
		}

		/// <summary>
		/// データを追加
		/// </summary>
		static void rangeData()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new();
			cmd.Connection = con;
			try
			{
				cmd.CommandText = @"INSERT INTO ""data""(name, numeric) VALUES ('a', 1);";
				var result1 = cmd.ExecuteNonQuery();
				cmd.CommandText = @"INSERT INTO ""data""(name, numeric) VALUES ('b', 10);";
				var result2 = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				// デフォルトパーティションテーブルが存在してないとパーティションテーブルの条件に一致しないINSERTがあると例外が発生
				// "23514: no partition of relation \"data\" found for row"
				string msg = exc.Message;
			}
		}
		#endregion

		#region 抽出した年LISTパーティション
		/// <summary>
		/// LISTパーティションを持ったテーブル
		/// </summary>
		static void listDateTable()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@"CREATE TABLE ""data""(time timestamp DEFAULT clock_timestamp(), name text, numeric integer) PARTITION BY LIST (date_part('year', time))", con);
			try
			{
				_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				string msg = exc.Message;
			}
		}

		/// <summary>
		/// LISTパーテイションテーブル作成
		/// テーブルの文字列でパーティション作成
		/// </summary>
		static void listDatePartition()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@$"CREATE TABLE _2020 PARTITION OF ""data"" FOR VALUES IN (2020);", con);
			_ = cmd.ExecuteNonQuery();
			cmd.CommandText = @$"CREATE TABLE _2021 PARTITION OF ""data"" FOR VALUES IN (2021);";
			_ = cmd.ExecuteNonQuery();
			// パーティションテーブルが存在しない値を入れるパーティションテーブル
			//cmd.CommandText = @$"CREATE TABLE default_name PARTITION OF ""data"" DEFAULT;";
			//_ = cmd.ExecuteNonQuery();
		}

		/// <summary>
		/// データを追加
		/// </summary>
		static void listDateData()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new();
			cmd.Connection = con;
			try
			{
				// 日付は自動取得でINSERT
				cmd.CommandText = @"INSERT INTO ""data""(name, numeric) VALUES ('a', 1);";
				var result1 = cmd.ExecuteNonQuery();
				// 日付を指定してINSERT
				cmd.CommandText = @"INSERT INTO ""data""(time, name, numeric) VALUES ('2020-10-10 10:20:30', 'b', 2);";
				var result2 = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				// デフォルトパーティションテーブルが存在してないとパーティションテーブルの条件に一致しないINSERTがあると例外が発生
				// "23514: no partition of relation \"data\" found for row"
				string msg = exc.Message;
			}
		}
		#endregion
		#endregion
	}
}
