using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.NetworkInformation;
using System.Reflection.PortableExecutable;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace PostgreSQL
{
	class Program
	{
		static void Main(string[] args)
		{
			/***** 接続 *****/
			// 通常の接続
			//open1();

			// NpgsqlConnectionにawait usingを使う
			//Task t2 = open2();
			//t2.Wait();

			// CancellationTokenSourceなしの非同期接続
			//Task t3 = open3();
			//t3.Wait();

			// CancellationTokenSourceありの非同期接続
			//using CancellationTokenSource tokenSource = new();
			//Task t4 = open4(tokenSource);
			//t4.Wait();

			// NpgsqlCommandで接続
			//open5();


			/***** テーブルの作成 *****/
			// 標準的なテーブル作成
			//newTable1();

			// テーブルが存在してなければテーブルの作成
			//newTable2();

			// LISTパーティションのあるテーブル作成
			//listTable();
			// メインテーブル下にパーティションを作成
			//listPartition("numeric_a");
			// データを追加
			//listData();

			// RANGEパーティションがあるテーブル作成
			//rangeTable();
			// メインテーブル下にパーティションを作成
			//rangePartition("range_numeric");
			// データを追加
			//rangeData();

			// 日付情報の年だけを見るLISTパーティションがあるテーブルの作成
			//listDateTable();
			// メインテーブル下にパーティションを作成
			//listDatePartition();
			// データを追加
			//listDateData();


			/***** INSERTのやり方 *****/
			// シンプルなINSERTのやり方
			//insert1("a", 1);

			// SQLインジェクション対策(の基になる手法)
			//insert2();

			// SQLインジェクション対策その2(の基になる手法)
			//insert3();

			// 配列(SELECTの例を含む)
			//insert4();


			/***** SELECTのやり方 *****/
			// シンプルなSELECTのやり方
			//select1();

			// カラムを番号で指定したSELECTで全てのカラムを読み込む
			//select2();

			// カラムを番号で指定してSELECTでは読み込むカラムを指定
			//select3();

			// NpgsqlCommand.FieldCountでカラム数を参照して取得
			//select4();

			// NpgsqlCommand.GetValueで取得
			//select5();

			// 型を明確にして取得
			//select6();

			// PostgreSQLのtimestamp型の時刻データを取得
			//select7();

			// 浮動小数点で問題点を列挙
			//select8();

			// boolean（論理型）
			//select9();

			// SQLインジェクションを避ける方法(の基になる手法)
			//select10();

			/***** トランザクションのやり方 *****/
			//transaction1();
			//transaction2();

			/***** prepare実験 *****/
			//selectTest1();
			//selectTest2();
			//selectTest3();
			//selectTest4();

			/***** NpgsqlDataAdapter実験 *****/
			//dataAdapterSelect1();
			//dataAdapterSelect2();
			//dataAdapterSelect3();
			//dataAdapterInsert1();
			//dataAdapterInsert2();
			//dataAdapterInsert3();
			//dataAdapterUpdate1();
			//dataAdapterUpdate2();
			//dataAdapterUpdate3();
			//dataAdapterDelete1(); 
			//dataAdapterDelete2();

			newTable2();
			binaryCopyInsert();
			binaryCopySelect();

			/***** テーブルの削除 *****/
			dropTable();

			Console.WriteLine("終了しました。");
			Console.ReadKey();
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

		#region テーブルの作成
		/// <summary>
		/// 標準的なテーブル作成
		/// </summary>
		static void newTable1()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("CREATE TABLE data(id serial PRIMARY KEY, time timestamp DEFAULT clock_timestamp(), name text, numeric integer)", con);
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
			// 「CREATE TABLE」の後にある「IF NOT EXISTS」が存在しない時だけ「CREATE TABLE」が実行され例外が発生しない
			using NpgsqlCommand cmd = new("CREATE TABLE IF NOT EXISTS data(id serial PRIMARY KEY, time timestamp DEFAULT clock_timestamp(), name text, numeric integer);", con);
			try
			{
				_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException)
			{

			}
		}
		#endregion

		#region LISTパーティション
		/// <summary>
		/// LISTパーティションを持ったテーブル
		/// </summary>
		static void listTable()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("CREATE TABLE data(time timestamp DEFAULT clock_timestamp(), name text, numeric integer) PARTITION BY LIST (name);", con);
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
			using NpgsqlCommand cmd = new(@$"CREATE TABLE {PartitionNname} PARTITION OF data FOR VALUES IN ('a');", con);
			_ = cmd.ExecuteNonQuery();
			// IF NOT EXISTSがあればパーティションテーブルの重複生成も抑制できる
			cmd.CommandText = @$"CREATE TABLE IF NOT EXISTS {PartitionNname} PARTITION OF data FOR VALUES IN ('a');";
			_ = cmd.ExecuteNonQuery();
			// パーティションテーブルが存在しない値を入れるパーティションテーブル
			cmd.CommandText = "CREATE TABLE default_name PARTITION OF data DEFAULT;";
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
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('a', 1);";
				int result1 = cmd.ExecuteNonQuery();
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('b', 2);";
				int result2 = cmd.ExecuteNonQuery();
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
			using NpgsqlCommand cmd = new("CREATE TABLE data(time timestamp DEFAULT clock_timestamp(), name text, numeric integer) PARTITION BY RANGE (numeric);", con);
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
				using NpgsqlCommand cmd = new(@$"CREATE TABLE {PartitionNname}_1 PARTITION OF data FOR VALUES FROM (1) TO (6);", con);
				_ = cmd.ExecuteNonQuery();
				cmd.CommandText = @$"CREATE TABLE {PartitionNname}_2 PARTITION OF data FOR VALUES FROM (6) TO (11);";
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
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('a', 1);";
				int result1 = cmd.ExecuteNonQuery();
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('b', 10);";
				int result2 = cmd.ExecuteNonQuery();
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
			using NpgsqlCommand cmd = new("CREATE TABLE data(time timestamp DEFAULT clock_timestamp(), name text, numeric integer) PARTITION BY LIST (date_part('year', time));", con);
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
			using NpgsqlCommand cmd = new("CREATE TABLE _2020 PARTITION OF data FOR VALUES IN (2020);", con);
			_ = cmd.ExecuteNonQuery();
			cmd.CommandText = "CREATE TABLE _2021 PARTITION OF data FOR VALUES IN (2021);";
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
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('a', 1);";
				int result1 = cmd.ExecuteNonQuery();
				// 日付を指定してINSERT
				cmd.CommandText = "INSERT INTO data(time, name, numeric) VALUES ('2020-10-10 10:20:30', 'b', 2);";
				int result2 = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				// デフォルトパーティションテーブルが存在してないとパーティションテーブルの条件に一致しないINSERTがあると例外が発生
				// "23514: no partition of relation \"data\" found for row"
				string msg = exc.Message;
			}
		}
		#endregion

		#region INSERT
		/// <summary>
		/// 標準的なINSERT
		/// </summary>
		/// <param name="insertName"></param>
		/// <param name="insertNumeric"></param>
		static void insert1(string insertName, int insertNumeric)
		{
			// テーブルを作る
			newTable1();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new($"INSERT INTO data(name, numeric) VALUES ({insertName}, {insertNumeric});", con);
			int result = cmd.ExecuteNonQuery();
		}

		/// <summary>
		/// １つのINSERTでシンプルなSQLインジェクション対策(の基になる手法)
		/// </summary>
		static void insert2()
		{
			// テーブルを作る
			newTable1();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			// SQLインジェクション対策
			using NpgsqlCommand cmd = new("INSERT INTO data(name, numeric) VALUES (@insert_name, @insert_numeric);", con);
			// 単発ならこれが簡単
			_ = cmd.Parameters.AddWithValue("insert_name", "a");
			_ = cmd.Parameters.AddWithValue("insert_numeric", 1);
			int result1 = cmd.ExecuteNonQuery();
			cmd.Parameters.Clear();
			_ = cmd.Parameters.AddWithValue("insert_name", "b");
			_ = cmd.Parameters.AddWithValue("insert_numeric", 2);
			int result2 = cmd.ExecuteNonQuery();
		}

		/// <summary>
		/// 複数のINSERTでシンプルなSQLインジェクション対策(の基になる手法)
		/// </summary>
		static void insert3()
		{
			// テーブルを作る
			newTable1();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			// SQLインジェクション対策
			using NpgsqlCommand cmd = new("INSERT INTO data(name, numeric) VALUES (@insert_name, @insert_numeric);", con);
			// NpgsqlCommandのParametersに新しいNpgsqlParameterを作成
			_ = cmd.Parameters.Add(new NpgsqlParameter("insert_name", DbType.String));
			_ = cmd.Parameters.Add(new NpgsqlParameter("insert_numeric", DbType.Int32));
			// INSERTする値をセット
			cmd.Parameters["insert_name"].Value = "b";
			cmd.Parameters["insert_numeric"].Value = 2;
			int result1 = cmd.ExecuteNonQuery();
			cmd.Parameters["insert_name"].Value = "c";
			cmd.Parameters["insert_numeric"].Value = 3;
			int result2 = cmd.ExecuteNonQuery();
		}

		/// <summary>
		/// 配列に対するINSERT
		/// </summary>
		static void insert4()
		{
			// INSERTする配列
			List<bool> input = new() { true, false, true };
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("CREATE TABLE data(result boolean[]);", con);
			_ = cmd.ExecuteNonQuery();
			// 標準的なINSERT
			cmd.CommandText = $"INSERT INTO data(result) VALUES (ARRAY[{input[0]}, {input[1]}, {input[2]}]);";
			_ = cmd.ExecuteNonQuery();
			// 配列変数をまとめてINSERT
			//cmd.CommandText = $"INSERT INTO data(result) VALUES (@array)";
			//cmd.Parameters.Add("array", NpgsqlDbType.Array | NpgsqlDbType.Boolean).Value = input;
			//_ = cmd.ExecuteNonQuery();
			cmd.CommandText = ("SELECT result FROM data;");
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			while (rd.Read())
			{
				bool[] result1 = (bool[])rd["result"];
				bool[] result2 = rd.GetFieldValue<bool[]>("result");
				List<bool> result3 = new((bool[])rd["result"]);
				List<bool> result4 = new(rd.GetFieldValue<bool[]>("result"));
			}
		}
		#endregion

		#region SELECT
		/// <summary>
		/// 基本的なSELECTのやり方
		/// </summary>
		static void select1()
		{
			// テーブルを作る
			newTable1();
			// データをSELECTする
			listData();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("SELECT * FROM data;", con);
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			while (rd.Read())
			{
				Console.WriteLine($"name:{rd["name"]} numeric:{rd["numeric"]}");
			}
		}

		/// <summary>
		/// SELECT * では全カラムの並び番号で取得
		/// </summary>
		static void select2()
		{
			// テーブルを作る
			newTable1();
			// データをINSERTする
			listData();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("SELECT * FROM data;", con);
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			while (rd.Read())
			{
				Console.WriteLine($"name:{rd[2]} numeric:{rd[3]}");
			}
		}

		/// <summary>
		/// SELECT name,numeric ではカラムの並び番号は0～で取得
		/// </summary>
		static void select3()
		{
			// テーブルを作る
			newTable1();
			// データをINSERTする
			listData();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("SELECT name,numeric FROM data;", con);
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			while (rd.Read())
			{
				Console.WriteLine($"name:{rd[0]} numeric:{rd[1]}");
			}
		}

		/// <summary>
		/// NpgsqlCommand.FieldCountでカラム数を参照して取得
		/// </summary>
		static void select4()
		{
			// テーブルを作る
			newTable1();
			// データをINSERTする
			listData();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("SELECT name,numeric FROM data;", con);
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			while (rd.Read())
			{
				for (int i = 0; i < rd.FieldCount; i++)
				{
					Console.WriteLine($"{rd[i]}");
				}
			}
		}

		/// <summary>
		/// DbDataReader.GetValueで取得
		/// </summary>
		static void select5()
		{
			// テーブルを作る
			newTable1();
			// データをINSERTする
			listData();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("SELECT name,numeric FROM data;", con);
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			while (rd.Read())
			{
				//Console.WriteLine($"name:{rd.GetValue(0)} numeric:{rd.GetValue(1)}");
				Console.WriteLine($"name:{rd.GetValue("name")} numeric:{rd.GetValue("numeric")}");
			}
		}

		/// <summary>
		/// 型を明確にして取得
		/// </summary>
		static void select6()
		{
			// テーブルを作る
			newTable1();
			// データをINSERTする
			listData();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new(@$"SELECT id,name,numeric FROM data;", con);
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			try
			{
				while (rd.Read())
				{
					// PostgreSQLではinteger型をGetInt16で取得可能だが符号あり16bitで表現できる数値以上だと例外「OverflowException」となる。
					//Console.WriteLine($"id:{rd.GetInt16(0)} name:{rd.GetString(1)}　numeric:{rd.GetInt32(2)}");
					// GetInt16,GetStringなどは引数にカラム名を用いて明確にできる(using System.Data;が必要)
					Console.WriteLine($"id:{rd.GetInt16("id")} name:{rd.GetString("name")}　numeric:{rd.GetInt32("numeric")}");
					// 数値をGetStringで取得すると「InvalidCastException」が発生する
					//Console.WriteLine($"id:{rd.GetInt16("id")} name:{rd.GetString("name")}　numeric:{rd.GetString("numeric")}");
				}
			}
			catch (InvalidCastException)
			{
				// 無効なキャストまたは明示的な型変換に対してスローされる例外
			}
			catch (OverflowException)
			{
				// 算術演算、キャスト演算、または変換演算の結果オーバーフローが発生した場合にスローされる例外
			}
		}

		/// <summary>
		/// 時間の取得
		/// </summary>
		static void select7()
		{
			// テーブルを作る
			newTable1();
			// データをINSERTする
			listData();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("SELECT time FROM data;", con);
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			try
			{
				while (rd.Read())
				{
					// PostgreSQLのtimestampをGetDateTimeメソッドで取得
					//Console.WriteLine($"time:{rd.GetDateTime(0)}");
					// GetDateTimeメソッドで取得した値はC#のDateTime型である
					//DateTime t = rd.GetDateTime(0);
					DateTime t = rd.GetDateTime("time");
					// GetTimeStampメソッドでも取得できるがNpgsqlDateTime型なので上記より利便性は落ちるかも
					//NpgsqlDateTime t = rd.GetTimeStamp(0);
				}
			}
			catch (InvalidCastException)
			{
				// 無効なキャストまたは明示的な型変換に対してスローされる例外
			}
			catch (OverflowException)
			{
				// 算術演算、キャスト演算、または変換演算の結果オーバーフローが発生した場合にスローされる例外
			}
		}

		/// <summary>
		/// 浮動小数点
		/// </summary>
		static void select8()
		{
			// テーブルを作る
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("CREATE TABLE data(real_data real, double_data double precision);", con);
			_ = cmd.ExecuteNonQuery();
			// データをINSERTする
			cmd.CommandText = "INSERT INTO data(real_data, double_data) VALUES (1.1, 1.1);";
			int result1 = cmd.ExecuteNonQuery();
			cmd.CommandText = ("SELECT * FROM data;");
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			try
			{
				while (rd.Read())
				{
					// 標準的なデータ取得
					{
						float value1 = rd.GetFloat("real_data");
						double value2 = rd.GetDouble("double_data");
						Console.WriteLine($"{value1} , {value2}");
					}

					// double precisionをGetFloatすると例外「InvalidCastException」
					//{
					//	float value1 = rd.GetFloat("real_data");
					//	float value2 = rd.GetFloat("double_data");
					//	Console.WriteLine($"{value1} , {value2}");
					//}

					// realをGetDoubleで取得できるがreal値1.1が1.100000023841858になる
					{
						double value1 = rd.GetDouble("real_data");
						double value2 = rd.GetDouble("double_data");
						Console.WriteLine($"{value1} , {value2}");
					}
				}
			}
			catch (InvalidCastException)
			{
				// 無効なキャストまたは明示的な型変換に対してスローされる例外
			}
			catch (OverflowException)
			{
				// 算術演算、キャスト演算、または変換演算の結果オーバーフローが発生した場合にスローされる例外
			}
		}

		/// <summary>
		/// boolean（論理型）
		/// </summary>
		static void select9()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			// テーブルを作る
			using NpgsqlCommand cmd = new("CREATE TABLE data(result boolean);", con);
			_ = cmd.ExecuteNonQuery();
			// データを文字でINSERTする
			cmd.CommandText = "INSERT INTO data(result) VALUES ('true');";
			_ = cmd.ExecuteNonQuery();
			// データをC#のbool型変数でINSERTする
			bool value = false;
			cmd.CommandText = $"INSERT INTO data(result) VALUES ({value});";
			_ = cmd.ExecuteNonQuery();
			cmd.CommandText = ("SELECT result FROM data;");
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			while (rd.Read())
			{
				bool result = rd.GetBoolean("result");
				Console.WriteLine($"{rd.GetBoolean("result")}");
			}
		}

		/// <summary>
		/// QLインジェクションを避ける方法(の基になる手法)
		/// </summary>
		static void select10()
		{
			// テーブルを作る
			newTable1();
			// データをINSERTする
			listData();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			// SQLインジェクション対策
			using NpgsqlCommand cmd = new($"SELECT * FROM data WHERE name = @data_name;", con);
			_ = cmd.Parameters.Add(new NpgsqlParameter("data_name", DbType.String));
			cmd.Parameters["data_name"].Value = "a";
			using NpgsqlDataReader rd = cmd.ExecuteReader();
			while (rd.Read())
			{
				Console.WriteLine($"name:{rd["name"]} numeric:{rd["numeric"]}");
			}
		}
		#endregion

		#region Transaction
		/// <summary>
		/// トランザクションを利用する(その1)
		/// </summary>
		static void transaction1()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			// con.Open()前に行うと例外「InvalidOperationException」になる。
			using NpgsqlTransaction tran = con.BeginTransaction();
			try
			{
				using NpgsqlCommand cmd = new("CREATE TABLE data(id serial PRIMARY KEY, time timestamp DEFAULT clock_timestamp(), name text, numeric integer);", con);
				_ = cmd.ExecuteNonQuery();
				// データをINSERTする
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('a', 1);";
				int result1 = cmd.ExecuteNonQuery();
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('b', 2);";
				int result2 = cmd.ExecuteNonQuery();
				tran.Commit();
				// Commitメソッドが実行（成功）した後にRollbackメソッドを実行しても例外「InvalidOperationException」になる。
				//tran.Rollback();
			}
			catch (PostgresException)
			{
				tran.Rollback();
			}
		}

		/// <summary>
		/// トランザクションを利用する(その2)
		/// </summary>
		static void transaction2()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			// con.Open()前に行うと例外「InvalidOperationException」になる。
			using NpgsqlTransaction tran = con.BeginTransaction();
			try
			{
				using NpgsqlCommand cmd = new("CREATE TABLE data(id serial PRIMARY KEY, time timestamp DEFAULT clock_timestamp(), name text, numeric integer);", con);
				cmd.Transaction = tran;
				_ = cmd.ExecuteNonQuery();
				// データをINSERTする
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('a', 1);";
				int result1 = cmd.ExecuteNonQuery();
				cmd.CommandText = "INSERT INTO data(name, numeric) VALUES ('b', 2);";
				int result2 = cmd.ExecuteNonQuery();
				cmd.Transaction.Commit();
			}
			catch (PostgresException)
			{
				tran.Rollback();
			}
		}

		#endregion

		#region Prepare
		/// <summary>
		/// Prepare実験用データ生成
		/// </summary>
		static void prepare()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlTransaction tran = con.BeginTransaction();
			using NpgsqlCommand cmd = new("CREATE TABLE IF NOT EXISTS data(id serial PRIMARY KEY, time timestamp DEFAULT clock_timestamp(), name text, numeric integer);", con);
			_ = cmd.ExecuteNonQuery();
			for (int i = 0; i < 1000; i++)
			{
				cmd.CommandText = $"INSERT INTO data(name, numeric) VALUES ('name_{i}', {i});";
				_ = cmd.ExecuteNonQuery();
			}
			tran.Commit();
		}

		/// <summary>
		/// SELECT文の繰り返し
		/// </summary>
		static void selectTest1()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new();
			cmd.Connection = con;
			System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
			List<TimeSpan> time = new();
			for (int i = 0; i < 1000; i++)
			{
				sw.Start();
				for (int j = 0; j < 1000; j++)
				{
					cmd.CommandText = $"SELECT name FROM data WHERE numeric = {j};";
					using NpgsqlDataReader rd = cmd.ExecuteReader();
					rd.Read();
					string result = rd.GetString("name");
				}
				sw.Stop();
				time.Add(sw.Elapsed);
				sw.Reset();
			}
			Console.WriteLine($"通常のSELECT繰り返し：{time.Average(t => t.TotalSeconds)}");
		}

		/// <summary>
		/// WHEREの条件をパラメータ化
		/// </summary>
		static void selectTest2()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new();
			cmd.Connection = con;
			System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
			List<TimeSpan> time = new();
			cmd.CommandText = "SELECT name FROM data WHERE numeric = @n;";
			_ = cmd.Parameters.Add(new NpgsqlParameter("n", NpgsqlDbType.Integer));
			for (int i = 0; i < 1000; i++)
			{
				sw.Start();
				for (int j = 0; j < 1000; j++)
				{
					cmd.Parameters["n"].Value = j;
					using NpgsqlDataReader rd = cmd.ExecuteReader();
					rd.Read();
					string result = rd.GetString("name");
				}
				sw.Stop();
				time.Add(sw.Elapsed);
				sw.Reset();
			}
			Console.WriteLine($"WHEREをパラメータにする：{time.Average(t => t.TotalSeconds)}");
		}

		/// <summary>
		/// Prepareメソッドを実行
		/// </summary>
		static void selectTest3()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new();
			cmd.Connection = con;
			System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
			List<TimeSpan> time = new();
			cmd.CommandText = "SELECT name FROM data WHERE numeric = @n;";
			_ = cmd.Parameters.Add(new NpgsqlParameter("n", NpgsqlDbType.Integer));
			cmd.Prepare();
			for (int i = 0; i < 1000; i++)
			{
				sw.Start();
				for (int j = 0; j < 1000; j++)
				{
					cmd.Parameters["n"].Value = j;
					using NpgsqlDataReader rd = cmd.ExecuteReader();
					rd.Read();
					string result = rd.GetString("name");
				}
				sw.Stop();
				time.Add(sw.Elapsed);
				sw.Reset();
			}
			Console.WriteLine($"Prepareメソッドを使う：{time.Average(t => t.TotalSeconds)}");
		}

		/// <summary>
		/// 接続文字列に「Max Auto Prepare」を設定
		/// </summary>
		static void selectTest4()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public; Max Auto Prepare=1");
			con.Open();
			using NpgsqlCommand cmd = new();
			cmd.Connection = con;
			System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
			List<TimeSpan> time = new();
			cmd.CommandText = "SELECT name FROM data WHERE numeric = @n;";
			_ = cmd.Parameters.Add(new NpgsqlParameter("n", NpgsqlDbType.Integer));
			for (int i = 0; i < 1000; i++)
			{
				sw.Start();
				for (int j = 0; j < 1000; j++)
				{
					cmd.Parameters["n"].Value = j;
					using NpgsqlDataReader rd = cmd.ExecuteReader();
					rd.Read();
					string result = rd.GetString("name");
				}
				sw.Stop();
				time.Add(sw.Elapsed);
				sw.Reset();
			}
			Console.WriteLine($"Max Auto PrepareはあるがPrepareメソッドがない：{time.Average(t => t.TotalSeconds)}");
		}
		#endregion

		#region NpgsqlDataAdapter
		/// <summary>
		///  NpgsqlDataAdapterを使ったSELECTその１
		/// </summary>
		static void dataAdapterSelect1()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT * FROM data;", con);
			// これでもOK
			//using NpgsqlDataAdapter nda = new("SELECT * FROM data;", "Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			var result = nda.Fill(dt);
			Console.WriteLine($"{dt.Columns[0].ColumnName},{dt.Columns[1].ColumnName},{dt.Columns[2].ColumnName},{dt.Columns[3].ColumnName}");
			for (int i = 0; i < dt.Rows.Count; i++)
			{
				Console.WriteLine($"{dt.Rows[i][0]},{dt.Rows[i][1]},{dt.Rows[i][2]},{dt.Rows[i][3]}");
			}
		}

		/// <summary>
		/// NpgsqlDataAdapterを使ったSELECTその2(取得するデータを指定)
		/// </summary>
		static void dataAdapterSelect2()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT name FROM data;", con);
			var result = nda.Fill(dt);
			// 取得可能なカラム数は「dt.Columns.Count」で取得可能
			Console.WriteLine($"{dt.Columns[0].ColumnName}");
			for (int i = 0; i < dt.Rows.Count; i++)
			{
				Console.WriteLine($"{dt.Rows[i][0]}");
			}
		}

		/// <summary>
		/// NpgsqlDataAdapterを使ったSELECTその3(取得する条件を指定)
		/// </summary>
		static void dataAdapterSelect3()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT * FROM data WHERE name = 'a';", con);
			var result = nda.Fill(dt);
			Console.WriteLine($"{dt.Columns[0].ColumnName},{dt.Columns[1].ColumnName},{dt.Columns[2].ColumnName},{dt.Columns[3].ColumnName}");
			for (int i = 0; i < dt.Rows.Count; i++)
			{
				Console.WriteLine($"{dt.Rows[i][0]},{dt.Rows[i][1]},{dt.Rows[i][2]},{dt.Rows[i][3]}");
			}
		}

		/// <summary>
		/// NpgsqlDataAdapterを使ったINSERT
		/// </summary>
		static void dataAdapterInsert1()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT * FROM data;", con);
			var result = nda.Fill(dt);
			// DataTableにデータの追加
			DataRow addRow = dt.NewRow();
			addRow[dt.Columns[2].ColumnName] = "c";
			addRow[dt.Columns[3].ColumnName] = 3;
			dt.Rows.Add(addRow);
			// データベースにINSERTするNpgsqlCommandを追加
			using NpgsqlCommand insertCommand = new();
			insertCommand.Connection = con;
			insertCommand.CommandText = "INSERT INTO data(name, numeric) VALUES(@Name, @Numeric);";
			// 「name」をINSERTするNpgsqlParameterを作成して追加
			NpgsqlParameter insertName = new();
			insertName.ParameterName = "@Name";
			insertName.SourceColumn = "name";
			_ = insertCommand.Parameters.Add(insertName);
			// 「numeric」をINSERTするNpgsqlParameterを作成して追加
			NpgsqlParameter insertNumeric = new();
			insertNumeric.ParameterName = "@Numeric";
			insertNumeric.SourceColumn = "numeric";
			_ = insertCommand.Parameters.Add(insertNumeric);
			// NpgsqlDataAdapterのInsertCommandに追加
			nda.InsertCommand = insertCommand;
			try
			{
				result = nda.Update(dt);
			}
			catch (DBConcurrencyException)
			{
				// INSERT、UPDATE、DELETE の各ステートメントを実行しようとしましたが、影響を受けたレコードがない
			}
		}

		/// <summary>
		/// NpgsqlDataAdapterを使いNpgsqlCommandBuilderでクエリを自動生成してINSERT(自動時刻にデータが入らない)
		/// </summary>
		static void dataAdapterInsert2()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT * FROM data;", con);
			var result = nda.Fill(dt);
			// DataTableにデータの追加
			DataRow addRow = dt.NewRow();
			addRow[dt.Columns[2].ColumnName] = "c";
			addRow[dt.Columns[3].ColumnName] = 3;
			dt.Rows.Add(addRow);
			// NpgsqlCommandBuilderを使うとカラム名「time」の自動時刻挿入「timestamp DEFAULT clock_timestamp()」がnullとなる。
			using NpgsqlCommandBuilder cb = new(nda);
			try
			{
				result = nda.Update(dt);
			}
			catch (DBConcurrencyException)
			{
				// INSERT、UPDATE、DELETE の各ステートメントを実行しようとしましたが、影響を受けたレコードがない
			}
			// NpgsqlCommandBuilderで生成されたクエリを見ると「INSERT INTO "db_PostgreTest"."public"."data" ("time", "name", "numeric") VALUES (@p1, @p2, @p3)」
			string str = cb.GetInsertCommand().CommandText;
		}

		/// <summary>
		/// NpgsqlDataAdapterを使いNpgsqlCommandBuilderでクエリを自動生成してINSERT(自動採番のidは何もしない)
		/// </summary>
		static void dataAdapterInsert3()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			// idは取得するがINSERTには含まれない
			using NpgsqlDataAdapter nda = new("SELECT id, name, numeric FROM data;", con);
			var result = nda.Fill(dt);
			// DataTableにデータの追加
			DataRow addRow = dt.NewRow();
			addRow[dt.Columns[1].ColumnName] = "c";
			addRow[dt.Columns[2].ColumnName] = 3;
			dt.Rows.Add(addRow);
			using NpgsqlCommandBuilder cb = new(nda);
			try
			{
				result = nda.Update(dt);
			}
			catch (DBConcurrencyException)
			{
				// INSERT、UPDATE、DELETE の各ステートメントを実行しようとしましたが、影響を受けたレコードがない
			}
			// これで実行されているクエリ文が分かる「INSERT INTO "db_PostgreTest"."public"."data" ("name", "numeric") VALUES (@p1, @p2)」
			string str = cb.GetInsertCommand().CommandText;
		}

		/// <summary>
		/// NpgsqlDataAdapterを使ったUPDATE
		/// </summary>
		static void dataAdapterUpdate1()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT * FROM data;", con);
			var result = nda.Fill(dt);
			// データを更新
			dt.Rows[0][3] = 100;
			// データベースにUPDATEするNpgsqlCommandを追加
			using NpgsqlCommand updateCommand = new();
			updateCommand.Connection = con;
			updateCommand.CommandText = "UPDATE data SET numeric = @numeric WHERE id = @id;";
			// 「id」を条件としてUPDATするNpgsqlParameterを作成して追加
			NpgsqlParameter updateId = new();
			updateId.ParameterName = "@id";
			updateId.SourceColumn = "id";
			_ = updateCommand.Parameters.Add(updateId);
			// UPDATEする「numeric」のNpgsqlParameterを作成して追加
			NpgsqlParameter updateNumeric = new();
			updateNumeric.ParameterName = "@numeric";
			updateNumeric.SourceColumn = "numeric";
			_ = updateCommand.Parameters.Add(updateNumeric);
			// NpgsqlDataAdapterのInsertCommandに追加
			nda.UpdateCommand = updateCommand;
			try
			{
				result = nda.Update(dt);
			}
			catch (DBConcurrencyException)
			{
				// INSERT、UPDATE、DELETE の各ステートメントを実行しようとしましたが、影響を受けたレコードがない
			}
		}

		/// <summary>
		/// NpgsqlDataAdapterを使いNpgsqlCommandBuilderでクエリを自動生成してUPDATE
		/// </summary>
		static void dataAdapterUpdate2()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT * FROM data;", con);
			var result = nda.Fill(dt);
			// データを更新
			dt.Rows[0][3] = 100;
			using NpgsqlCommandBuilder cb = new(nda);
			try
			{
				result = nda.Update(dt);
			}
			catch (DBConcurrencyException)
			{
				// INSERT、UPDATE、DELETE の各ステートメントを実行しようとしましたが、影響を受けたレコードがない
			}
			// これで実行されているクエリ文が分かる「UPDATE "db_PostgreTest"."public"."data" SET "time" = @p1, "name" = @p2, "numeric" = @p3 WHERE (("id" = @p4) AND ((@p5 = 1 AND "time" IS NULL) OR ("time" = @p6)) AND ((@p7 = 1 AND "name" IS NULL) OR ("name" = @p8)) AND ((@p9 = 1 AND "numeric" IS NULL) OR ("numeric" = @p10)))」
			string str = cb.GetUpdateCommand().CommandText;
		}

		/// <summary>
		/// NpgsqlDataAdapterを使いNpgsqlCommandBuilderでクエリを自動生成してUPDATE
		/// </summary>
		static void dataAdapterUpdate3()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT id, name, numeric FROM data;", con);
			var result = nda.Fill(dt);
			// データを更新
			dt.Rows[0][2] = 100;
			using NpgsqlCommandBuilder cb = new(nda);
			try
			{
				result = nda.Update(dt);
			}
			catch (DBConcurrencyException)
			{
				// INSERT、UPDATE、DELETE の各ステートメントを実行しようとしましたが、影響を受けたレコードがない
			}
			// これで実行されているクエリ文が分かる「UPDATE "db_PostgreTest"."public"."data" SET "name" = @p1, "numeric" = @p2 WHERE (("id" = @p3) AND ((@p4 = 1 AND "name" IS NULL) OR ("name" = @p5)) AND ((@p6 = 1 AND "numeric" IS NULL) OR ("numeric" = @p7)))」
			string str = cb.GetUpdateCommand().CommandText;
		}

		/// <summary>
		/// NpgsqlDataAdapterを使ったDELETE
		/// </summary>
		static void dataAdapterDelete1()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT * FROM data;", con);
			var result = nda.Fill(dt);
			// データを削除
			dt.Rows[0].Delete();
			// データベースにDELETEするNpgsqlCommandを追加
			using NpgsqlCommand deleteCommand = new();
			deleteCommand.Connection = con;
			deleteCommand.CommandText = "DELETE FROM data WHERE id = @id;";
			// 「id」を条件としてDELETEするNpgsqlParameterを作成して追加
			NpgsqlParameter deleteId = new();
			deleteId.ParameterName = "@id";
			deleteId.SourceColumn = "id";
			_ = deleteCommand.Parameters.Add(deleteId);
			// NpgsqlDataAdapterのInsertCommandに追加
			nda.DeleteCommand = deleteCommand;
			try
			{
				result = nda.Update(dt);
			}
			catch (DBConcurrencyException)
			{
				// INSERT、UPDATE、DELETE の各ステートメントを実行しようとしましたが、影響を受けたレコードがない
			}
		}

		/// <summary>
		/// NpgsqlDataAdapterを使いNpgsqlCommandBuilderでクエリを自動生成してDELETE
		/// </summary>
		static void dataAdapterDelete2()
		{
			using DataTable dt = new();
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlDataAdapter nda = new("SELECT * FROM data;", con);
			var result = nda.Fill(dt);
			// データを削除
			dt.Rows[0].Delete();
			using NpgsqlCommandBuilder cb = new(nda);
			try
			{
				result = nda.Update(dt);
			}
			catch (DBConcurrencyException)
			{
				// INSERT、UPDATE、DELETE の各ステートメントを実行しようとしましたが、影響を受けたレコードがない
			}
			// これで実行されているクエリ文が分かる「DELETE FROM "db_PostgreTest"."public"."data" WHERE (("id" = @p1) AND ((@p2 = 1 AND "time" IS NULL) OR ("time" = @p3)) AND ((@p4 = 1 AND "name" IS NULL) OR ("name" = @p5)) AND ((@p6 = 1 AND "numeric" IS NULL) OR ("numeric" = @p7)))」
			string str = cb.GetDeleteCommand().CommandText;
		}
		#endregion

		#region Binary copy
		/// <summary>
		/// バイナリーコピーで書き込み
		/// </summary>
		static void binaryCopyInsert()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			System.Diagnostics.Stopwatch sw = new();
			sw.Start();
			using NpgsqlBinaryImporter writer = con.BeginBinaryImport("COPY data (name, numeric) FROM STDIN (FORMAT BINARY)");
			for (int i = 0; i < 100000; i++)
			{
				try
				{
					// 1つの行の書き込みを開始し、列を書き込む前に起動しなければなりません。
					writer.StartRow();
					// 書き込む値と型
					writer.Write($"name{i}", NpgsqlDbType.Text);
					writer.Write(i, NpgsqlDbType.Integer);
					
				}
				catch (InvalidOperationException)
				{
					// BeginBinaryImportで２つのカラムを指定しているのにStartRowメソッド以降Writeメソッドが不足して次のStartRowメソッドを実行すると例外「InvalidOperationException」が発生
				}
			}
			// コピー開始
			try
			{
				writer.Complete();
			}
			catch (PostgresException)
			{
				// 違う型のデータを書き込もうとするとCompleteメソッドで例外「PostgresException」が発生
			}
			sw.Stop();
			Console.WriteLine($"{sw.Elapsed}");
		}

		/// <summary>
		/// バイナリーコピーで読み込み
		/// </summary>
		static void binaryCopySelect()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlBinaryExporter reader = con.BeginBinaryExport("COPY data (name, numeric) TO STDOUT (FORMAT BINARY)");
			System.Diagnostics.Stopwatch sw = new();
			sw.Start();
			while (reader.StartRow() != -1)
			{
				try
				{
					string s = reader.Read<string>(NpgsqlDbType.Text);
					int i = reader.Read<int>(NpgsqlDbType.Integer);
				}
				catch (NullReferenceException)
				{
					// 型が違うと例外が発生
				}
				catch (InvalidCastException)
				{
					// Readメソッドで取得したデータがnullだとキャストの例外が発生
					// reader.Read<string>(NpgsqlDbType.Integer);などの型が不一致だと例外が発生
				}
			}
			sw.Stop();
			Console.WriteLine($"{sw.Elapsed}");
		}
		#endregion

		#region Text copy
		/// <summary>
		/// テキストコピーで書き込み
		/// </summary>
		static void textCopyInsert()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			System.Diagnostics.Stopwatch sw = new();
			sw.Start();
			try
			{
				// 実際にデータベースに書き込まれるのはusingを抜けたタイミング
				using System.IO.TextWriter writer = con.BeginTextImport("COPY data (name, numeric) FROM STDIN WITH csv");
				for (int i = 0; i < 100000; i++)
				{
					// BeginTextImportの引数文字列に「WITH csv」がない場合は区切りはタブ、ある場合はカンマ 改行は\n
					writer.Write($"name{i},{i}\n");
				}
			}
			catch (PostgresException)
			{
				// 無効なデータ(integerに数値変換できないテキストなど)だと例外が発生
			}
			
			sw.Stop();
			Console.WriteLine($"{sw.Elapsed}");
		}

		/// <summary>
		/// バイナリーコピーで読み込み
		/// </summary>
		static void textCopySelect()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using System.IO.TextReader reader = con.BeginTextExport("COPY data(name, numeric) TO STDOUT WITH csv");
			System.Diagnostics.Stopwatch sw = new();
			sw.Start();
			string str;
			int count = 0;
			while ((str = reader.ReadLine()) != null)
			{
				count++;
				string[] selectData = str.Split(',');
			}
			sw.Stop();
			Console.WriteLine($"{sw.Elapsed}");
		}
		#endregion

		/// <summary>
		/// テーブルを削除
		/// </summary>
		static void dropTable()
		{
			using NpgsqlConnection con = new("Server=127.0.0.1; Port=5432; User Id=test_user; Password=pass; Database=db_PostgreTest; SearchPath=public");
			con.Open();
			using NpgsqlCommand cmd = new("DROP TABLE IF EXISTS data;", con);
			try
			{
				_ = cmd.ExecuteNonQuery();
			}
			catch (PostgresException exc)
			{
				// 「IF EXISTS」を加えないと存在しないテーブルを削除で例外
				string msg = exc.Message;
			}
		}
	}
}
