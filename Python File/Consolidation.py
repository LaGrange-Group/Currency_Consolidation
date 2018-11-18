import MySQLdb, telegram

conn=MySQLdb.connect(host='localhost', user='root',passwd='###', db='datadb')
cursor = conn.cursor()

class Consolidation_Binance:

    def run():

    # Scan database for two (2) day consolidation -
    # 5060 represents x which is the length of time you are checking past data (each batch represents data capture 30 sec apart)
    # 0.015 represents y which is the percentage differance from price x amount of days ago
        def scan_for_consolidation():
            consolidation_scan_query = """INSERT INTO consolidation (TOKEN, BATCH_KEY)
                                            SELECT A.TICKER AS TOKEN
                                            	   ,D.CURRENT_KEY


                                            	FROM
                                            		(SELECT TICKER, PRICE, BATCH_KEY
                                            		FROM storage
                                            		WHERE BATCH_KEY = (SELECT (MAX(BATCH_KEY) - 5060) FROM batch)) A

                                            		INNER JOIN (SELECT TICKER, MAX(PRICE) AS MAX_PRICE
                                            		FROM storage
                                                    WHERE BATCH_KEY BETWEEN (SELECT (MAX(BATCH_KEY) - 5060) FROM batch) AND (SELECT MAX(BATCH_KEY) FROM batch)
                                            		GROUP BY TICKER) B
                                            		ON A.TICKER = B.TICKER

                                            		INNER JOIN (SELECT TICKER, MIN(PRICE) AS MIN_PRICE
                                            		FROM storage
                                                    WHERE BATCH_KEY BETWEEN (SELECT (MAX(BATCH_KEY) - 5060) FROM batch) AND (SELECT MAX(BATCH_KEY) FROM batch)
                                            		GROUP BY TICKER) C
                                            		ON A.TICKER = C.TICKER

                                            		INNER JOIN (SELECT MAX(BATCH_KEY) AS CURRENT_KEY
                                            		FROM batch) D

                                            WHERE B.MAX_PRICE < (A.PRICE * 0.015 + A.PRICE)
                                            AND C.MIN_PRICE > ((A.PRICE * 0.015 + (A.PRICE * -1)) * -1)
                                            AND A.TICKER NOT IN (SELECT TOKEN FROM consolidation)
                                            AND A.TICKER NOT IN ('BCCBTC')"""
            cursor.execute(consolidation_scan_query)
            conn.commit()

    # Send results to telegram channel
        def send_tele_sig(msg):
        	bot = telegram.Bot(token='###')
        	bot.sendMessage(chat_id='@consolidationbinance', text=msg)

    # Truncate consolidation table
        def truncate_consolidation_table():
            cursor.execute("TRUNCATE TABLE consolidation")
            conn.commit()

    # Get highest BATCH_KEY
        def set_max_consolidation_batch():
            cursor.execute("SELECT MAX(BATCH_KEY) AS maximum FROM consolidation")
            result = cursor.fetchall()
            for i in result:
                batch_id_fn = (i[0])
            return batch_id_fn

    # Set python variable equal to the highest BATCH_KEY column value in batch table
        def set_new_batchid():
            cursor.execute("SELECT MAX(BATCH_KEY) AS maximum FROM batch")
            result = cursor.fetchall()
            for i in result:
                batch_id_fn = (i[0])
            return batch_id_fn

    # Get all results that have highest BATCH_KEY
        def collect_new_results_table():
            cursor.execute("SELECT TOKEN FROM consolidation WHERE BATCH_KEY = (SELECT MAX(BATCH_KEY) FROM consolidation)")
            result = cursor.fetchall()
            results_of_table = result
            return results_of_table

        scan_for_consolidation()
        consolidation_key = set_max_consolidation_batch()
        batch_key = set_new_batchid()
        new_results = collect_new_results_table()

        if batch_key == consolidation_key:
            send_tele_sig(str(new_results) + " Consolidation TEST")
