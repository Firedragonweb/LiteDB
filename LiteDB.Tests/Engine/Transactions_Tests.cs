﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Text.RegularExpressions;
using LiteDB.Engine;
using System.Threading;

namespace LiteDB.Tests.Engine
{
    [TestClass]
    public class Transactions_Tests
    {
        [TestMethod]
        public void Transaction_Write_Lock_Timeout()
        {
            var data1 = DataGen.Person().Take(100).ToArray();
            var data2 = DataGen.Person().Skip(100).Take(100).ToArray();

            using (var db = new LiteEngine(new EngineSettings { Timeout = TimeSpan.FromSeconds(1) }))
            {
                // init person collection with 100 document
                db.Insert("person", data1);

                // task A will open transaction and will insert +100 documents 
                // but will commit only 2s later
                var ta = new Task(() =>
                {
                    db.BeginTrans();

                    db.Insert("person", data2);

                    Task.Delay(2000).Wait();

                    var count = db.Count("person", "1 = 1");

                    Assert.AreEqual(data1.Length + data2.Length, count);

                    db.Commit();
                });

                // task B will try delete all documents but will be locked during 1 second
                var tb = new Task(() =>
                {
                    Task.Delay(250).Wait();

                    db.BeginTrans();

                    try
                    {
                        db.Delete("person", BsonExpression.Create("1=1"));

                        Assert.Fail("Must be locked");
                    }
                    catch(LiteException ex) when (ex.ErrorCode == LiteException.LOCK_TIMEOUT)
                    {
                        // ok!
                    }
                    catch(Exception ex)
                    {
                        Assert.Fail(ex.Message);
                    }

                });

                ta.Start();
                tb.Start();

                Task.WaitAll(ta, tb);

                // check if no pending transaction
                Assert.AreEqual(0, db.Count("$transactions"));
            }
        }


        [TestMethod]
        public void Transaction_Avoid_Drity_Read()
        {
            var data1 = DataGen.Person().Take(100).ToArray();
            var data2 = DataGen.Person().Skip(100).Take(100).ToArray();

            using (var db = new LiteEngine())
            {
                // init person collection with 100 document
                db.Insert("person", data1);

                // task A will open transaction and will insert +100 documents 
                // but will commit only 1s later - this plus +100 document must be visible only inside task A
                var ta = new Task(() =>
                {
                    db.BeginTrans();

                    db.Insert("person", data2);

                    Task.Delay(1000).Wait();

                    var count = db.Count("person", "1 = 1");

                    Assert.AreEqual(data1.Length + data2.Length, count);

                    db.Commit();
                });

                // task B will not open transaction and will wait 250ms before and count collection - 
                // at this time, task A already insert +100 document but here I cann't see (are not commited yet)
                // after task A finish, I can see now all 200 documents
                var tb = new Task(() =>
                {
                    Task.Delay(250).Wait();

                    var count = db.Count("person", "1 = 1"); // using 1=1 to force full scan

                    // read 100 documents
                    Assert.AreEqual(data1.Length, count);

                    ta.Wait();

                    // read 200 documets
                    count = db.Count("person", "1 = 1");

                    Assert.AreEqual(data1.Length + data2.Length, count);
                });

                ta.Start();
                tb.Start();

                Task.WaitAll(ta, tb);

                // check if no pending transaction
                Assert.AreEqual(0, db.Count("$transactions"));
            }
        }

        [TestMethod]
        public void Transaction_Read_Version()
        {
            var data1 = DataGen.Person().Take(100).ToArray();
            var data2 = DataGen.Person().Skip(100).Take(100).ToArray();

            using (var db = new LiteEngine())
            {
                // init person collection with 100 document
                db.Insert("person", data1);

                // task A will insert more 100 documents but will commit only 1s later
                var ta = new Task(() =>
                {
                    db.BeginTrans();

                    db.Insert("person", data2);

                    Task.Delay(1000).Wait();

                    db.Commit();
                });

                // task B will open transaction too and will count 100 original documents only
                // but now, will wait task A finish - but is in transaction and must see only initial version
                var tb = new Task(() =>
                {
                    db.BeginTrans();

                    Task.Delay(250).Wait();

                    var count = db.Count("person", "1 = 1"); // using 1=1 to force full scan

                    // read 100 documents
                    Assert.AreEqual(data1.Length, count);

                    ta.Wait();

                    // keep reading 100 documets because i'm still in same transaction
                    count = db.Count("person", "1 = 1");

                    Assert.AreEqual(data1.Length, count);
                });

                ta.Start();
                tb.Start();

                Task.WaitAll(ta, tb);
            }
        }

        [TestMethod]
        public void Avoid_Two_Transactions_In_Same_Thread()
        {
            using (var db = new LiteEngine())
            {
                db.BeginTrans();

                try
                {
                    db.BeginTrans();

                    Assert.Fail("Do not accept second begin");
                }
                catch(LiteException ex) when(ex.ErrorCode == LiteException.INVALID_TRANSACTION_STATE)
                {
                    // ok!
                }
                catch(Exception ex)
                {
                    Assert.Fail(ex.Message);
                }
            }
        }

        [TestMethod]
        public void Avoid_Commit_Without_Transaction()
        {
            using (var db = new LiteEngine())
            {
                try
                {
                    db.Commit();

                    Assert.Fail("Do not accept Commit without Begin transaction");
                }
                catch (LiteException ex) when (ex.ErrorCode == LiteException.INVALID_TRANSACTION_STATE)
                {
                    // ok!
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message);
                }
            }
        }

        [TestMethod]
        public void Avoid_Rollback_Without_Transaction()
        {
            using (var db = new LiteEngine())
            {
                try
                {
                    db.Rollback();

                    Assert.Fail("Do not accept Rollback without Begin transaction");
                }
                catch (LiteException ex) when (ex.ErrorCode == LiteException.INVALID_TRANSACTION_STATE)
                {
                    // ok!
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message);
                }
            }
        }
    }
}