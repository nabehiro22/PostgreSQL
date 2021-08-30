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


		}

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

	}
}
