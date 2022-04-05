import requests
import json
from random import randrange
import psycopg2
import time
import allure
from clickhouse_driver import Client
import configparser  # импортируем библиотеку

config = configparser.ConfigParser()  # создаём объекта парсера
config.read("settings.ini", encoding='utf-8')  # читаем конфиг

srt = '+78'
bankMnemocode = '00000010'
optionId = 'PS4'
conts_time = int(config["time"]["time1"])
x = config["Clickhouse"]["table"]

url_list = [config["URL"]["url0"],
            config["URL"]["url1"],
            config["URL"]["url2"],
            config["URL"]["url3"]
            ]
headers = {
    'Content-Type': 'application/json'

}

CodeList = [
    # "2-MWPS_PBA", "",
    # "2-MCPS_DG", "",
    # "2-MCPS_PBA_NON", "",
    # "2-MWPSA_PBA", "",
    "2-VPPS_PBA", "",
    # "2-VPPS_NON", "",
    # "2-VPPSA_PBA", "",
    # "2-VPPS3_PBA","",
    # "2-VPPSA_PBA","",

]


class User:
    def __init__(self):
        # определить все переменные
        i = randrange(100008000, 1000008050)
        number = srt + str(i)
        cardholderId = int(number) % 100000000
        self.number = number
        self.cardholderId = cardholderId
        self.i = i

    def get_number(self):
        return self.number

    def get_cardholderId(self):
        return self.cardholderId

    def get_i(self):
        return self.i


class GeneratorUser(User):
    def __init__(self, mcc_list, z_list, currency, parametr_option):
        for k in range(0, int(len(CodeList) / 2)):
            g = User()
            number = g.get_number()
            cardholderId = g.get_cardholderId()
            i = g.get_i()
            payload = json.dumps([
                {
                    "bankMnemocode": bankMnemocode,
                    "fname": "Auto",
                    "lname": "test",
                    "dateOfBirth": "1990-01-01",
                    "sex": "M",
                    "phone1": number,
                    "email1": "leo@mail.ru",
                    "profileClosed": "N",
                    "cardholderId": str(cardholderId)
                }
            ])
            if parametr_option == "cashback":
                payload_2 = json.dumps([
                    {
                        "bankMnemocode": bankMnemocode,
                        "cardName": "3822 7565552",
                        "cardId": '42763800' + str(cardholderId),
                        "cardProductId": CodeList[3 * k],
                        #"loyaltyMnemocode": '99990017',
                        "cardIndicator": "A",
                        "loyaltyId": "",
                        "loyaltySignupFlag": "N",
                        "cardholderId": str(cardholderId),
                        "attribute1": CodeList[3 * k + 1],
                        "binding": "auto",
                        "optionId": optionId

                    }
                ])
            elif parametr_option == "mili":
                payload_2 = json.dumps([
                    {
                        "bankMnemocode": bankMnemocode,
                        "cardName": "3822 7565552",
                        "cardId": '42763800' + str(cardholderId),
                        "cardProductId": CodeList[3 * k],
                        "loyaltyMnemocode": '99990017',
                        "cardIndicator": "A",
                        "loyaltyId": "",
                        "loyaltySignupFlag": "N",
                        "cardholderId": str(cardholderId),
                        "attribute1": CodeList[3 * k + 1],
                        "binding": "auto",
                        "optionId": optionId

                    }
                ])
            elif parametr_option == "s7":
                payload_2 = json.dumps([
                    {
                        "bankMnemocode": bankMnemocode,
                        "cardName": "3822 7565552",
                        "cardId": '42763800' + str(cardholderId),
                        "cardProductId": CodeList[3 * k],
                        "loyaltyMnemocode": '99990002',
                        "cardIndicator": "A",
                        "loyaltyId": "",
                        "loyaltySignupFlag": "N",
                        "cardholderId": str(cardholderId),
                        "attribute1": CodeList[3 * k + 1],
                        "binding": "auto",
                        "optionId": optionId

                    }
                ])
            elif parametr_option == 'perekrestok':
                payload_2 = json.dumps([
                    {
                        "bankMnemocode": bankMnemocode,
                        "cardName": "3822 7565552",
                        "cardId": '42763800' + str(cardholderId),
                        "cardProductId": CodeList[3 * k],
                        "loyaltyMnemocode": '99990003',
                        "cardIndicator": "A",
                        "loyaltyId": "",
                        "loyaltySignupFlag": "N",
                        "cardholderId": str(cardholderId),
                        "attribute1": CodeList[3 * k + 1],
                        "binding": "auto",
                        "optionId": optionId

                    }
                ])

            response = requests.request("POST", url_list[0], headers=headers, data=payload)
            status_code_1 = response.status_code
            json_response = response.json()
            dict_answer = json_response[0]
            dict_responseObj = dict_answer.get('responseObject')
            resultMessage = dict_answer.get('resultMessage')
            mnemocode = dict_responseObj.get('mnemocode')
            profileId = dict_responseObj.get('profileId')
            self.resultMessage = resultMessage
            self.status_code_1 = status_code_1
            self.dict_answer = dict_answer
            self.profileId = profileId

            response_2 = requests.request("POST", url_list[1], headers=headers, data=payload_2)
            status_code_2 = response_2.status_code
            json_response_2 = response_2.json()
            dict_answer_2 = json_response_2[0]
            requestObject = dict_answer_2.get('requestObject')
            responseObject_2 = dict_answer_2.get('responseObject')
            cardId = requestObject.get('cardId')
            cardMnemocode = responseObject_2.get('mnemocode')
            resultMessage_2 = dict_answer_2.get('resultMessage')
            self.status_code_2 = status_code_2
            self.dict_answer_2 = dict_answer_2
            self.resultMessage_2 = resultMessage_2
            self.cardId = cardId
            self.cardMnemocode = cardMnemocode

            print(CodeList[2 * k], CodeList[2 * k + 1], number, cardId, cardholderId, mnemocode,
                  cardMnemocode, profileId)
            print(resultMessage)
            print(resultMessage_2)
            sum_2 = 0
            for p in range(0, 6):
                # закодить ConfigParser через database.ini
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                millis = int(round(time.time() * 1000))

                # create a cursor

                # execute a statement
                # display the PostgreSQL database server version
                # exp
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE  apps.calc_stop_list  SET end_date=(%s)"
                    " WHERE card_external_id  = (%s)",
                    ('2021-06-30', cardId,))
                conn.commit()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE  apps.user_options  SET start_date=(%s)"
                    " WHERE profile_id  = (%s)",
                    ('2021-04-19 00:00:00', profileId,))
                conn.commit()

                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE apps.t_entry_combination  SET combination_date=(%s)"
                    " WHERE profile_id  = (%s)",
                    ('2021-04-19', profileId,))
                conn.commit()

                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE apps.t_entry_combination  SET combination_datetime=(%s)"
                    " WHERE profile_id  = (%s)",
                    ('2021-04-19 12:02:20', profileId,));
                conn.commit()

                cursor.execute(
                    "INSERT INTO ftype_writer.ftype_cl (id, mnemocode, mode, first_name, last_name, middle_name, "
                    "birthday, "
                    "sex, phone, stop_flag, closed_flag, cardholder_id, bank_mnemocode, created_when) VALUES(%s, %s, "
                    "%s, "
                    "%s, "
                    "%s, %s, %s, %s, %s, %s, %s , %s , %s , %s  )",
                    (millis, mnemocode, 'P', 'test_name', 'Auto', 'Test', '1990-01-01', 'F', number, 'N', 'N',
                     cardholderId,
                     bankMnemocode,
                     '2020-12-18 11:09:39'))
                conn.commit()

                cursor.execute(
                    "INSERT INTO ftype_writer.ftype_pc (id, mnemocode, mode, card_name, product_id, is_active, "
                    "card_id, "
                    "cardholder_id, bank_mnemocode, created_when) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s )",
                    (millis, mnemocode, 'P', 'test_name', 1002785, 'A', cardId, cardholderId, bankMnemocode,
                     '2020-12-18 11:09:39'))
                conn.commit()

                print(millis)
                sum_2 += z_list[p]
                self.sum_2 = sum_2
                if currency == 810:
                    if z_list[p] > 0:
                        cursor.execute(
                            "INSERT INTO  ftype_writer.ftype_tf (id, tr_date, tr_amount, tr_ref_number, "
                            "partner_mnemocode, "
                            "confirmed, card_id, tr_time, tr_currency, tr_amount_rub, tr_type,"
                            "merch_category_code, merch_id_num, terminal_id_num, retrieval_ref_num, created_when, "
                            "delivered, "
                            "date_posting) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (
                                millis, '2021-09-01', abs(z_list[p]), '76584736528', bankMnemocode, 'true', cardId,
                                '14:45',
                                '810',
                                abs(z_list[p]),
                                'O', mcc_list[p], '83878661648', '47832666', '745370807207', '2021-07-27 11:10:39',
                                'false',
                                '2021-09-01'))
                        conn.commit()
                    else:
                        cursor.execute(
                            "INSERT INTO  ftype_writer.ftype_tf (id, tr_date, tr_amount, tr_ref_number, "
                            "partner_mnemocode, "
                            "confirmed, card_id, tr_time, tr_currency, tr_amount_rub, tr_type,"
                            "merch_category_code, merch_id_num, terminal_id_num, retrieval_ref_num, created_when, "
                            "delivered, "
                            "date_posting) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (
                                millis, '2021-09-01', abs(z_list[p]), '76584736528', bankMnemocode, 'true', cardId,
                                '14:45',
                                '810',
                                abs(z_list[p]),
                                'R', mcc_list[p], '83878661648', '47832666', '745370807207', '2021-07-27 11:10:39',
                                'false',
                                '2021-09-01'))
                        conn.commit()
                else:
                    if z_list[p] > 0:
                        cursor.execute(
                            "INSERT INTO  ftype_writer.ftype_tf (id, tr_date, tr_amount, tr_ref_number, "
                            "partner_mnemocode, "
                            "confirmed, card_id, tr_time, tr_currency, tr_amount_rub, tr_type,"
                            "merch_category_code, merch_id_num, terminal_id_num, retrieval_ref_num, created_when, "
                            "delivered, "
                            "date_posting) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (
                                millis, '2021-09-01', abs(z_list[p]), '76584736528', bankMnemocode, 'true', cardId,
                                '14:45',
                                '840', abs(z_list[p]) * 73.87811,
                                'O', mcc_list[p], '83878661648', '47832666', '745370807207', '2021-07-27 11:10:39',
                                'false',
                                '2021-09-01'))
                        conn.commit()
                    else:
                        cursor.execute(
                            "INSERT INTO  ftype_writer.ftype_tf (id, tr_date, tr_amount, tr_ref_number, "
                            "partner_mnemocode, "
                            "confirmed, card_id, tr_time, tr_currency, tr_amount_rub, tr_type,"
                            "merch_category_code, merch_id_num, terminal_id_num, retrieval_ref_num, created_when, "
                            "delivered, "
                            "date_posting) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (
                                millis, '2021-09-01', abs(z_list[p]), '76584736528', bankMnemocode, 'true', cardId,
                                '14:45',
                                '840', abs(z_list[p]) * 73.8781,
                                'R', mcc_list[p], '83878661648', '47832666', '745370807207', '2021-07-27 11:10:39',
                                'false',
                                '2021-09-01'))
                        conn.commit()

            payload_3 = json.dumps({
                "partnerMnemocode": bankMnemocode,
                "cardholderId": cardholderId,
                "endDate": "2021-09-01T22:50:37.079Z",
                "prcMode": "CREATION",
                "cardIds": [cardId
                            ],
                "dryRun": False,
                "testCalc": False
            })
            response_3 = requests.request("POST", url_list[2], headers=headers, data=payload_3)

    def get_resultMessage(self):
        return self.resultMessage

    def get_resultMessage_2(self):
        return self.resultMessage_2

    def get_statuscode_1(self):
        return self.status_code_1

    def get_statuscode_2(self):
        return self.get_statuscode_2

    def get_dict_answer(self):
        return self.dict_answer

    def get_dict_answer_2(self):
        return self.dict_answer_2

    def get_ft_records(self):
        return self.ft_records

    def get_cardId(self):
        return self.cardId

    def get_sum_2(self):
        return self.sum_2

    def get_profileId(self):
        return self.profileId

    def get_cardMnemocode(self):
        return self.cardMnemocode


def test_CreateUser():
    g = GeneratorUser(mcc_list=["5811", "5812", "5813", "5814", "5137", "7999"],
                      z_list=[40000, 598698, 2345345, 234562323, 34523452, 563212], currency=810,
                      parametr_option="cashback")
    with allure.step(f"Запрос на создании клиента отправлен. Код ответа {g.get_statuscode_1()}"):
        assert g.get_statuscode_1() == 200, f"Неверный код ответа, получен {g.get_statuscode_1()}"
    with allure.step(f"Запрос на создании клиента отправлен. Ответ ручки {g.get_dict_answer()}"):
        assert g.get_dict_answer() != '0'
    with allure.step(f"Запрос на создании клиента отправлен.Ответ статуса {g.get_resultMessage()}"):
        assert g.get_resultMessage() == 'OK'

    with allure.step(f"Запрос на создании карты отправлен.Ответ ручки {g.get_dict_answer_2()}"):
        assert g.get_dict_answer_2() != '0'
    with allure.step(f"Запрос на создании карты отправлен.Ответ статуса {g.get_resultMessage_2()}"):
        assert g.get_resultMessage_2() == 'OK'


def test_checkSum():
    g = GeneratorUser(mcc_list=["5811", "5812", "5813", "5814", "5137", "7999"],
                      z_list=[40000, 598698, 2345345, 234562323, 34523452, 563212], currency=810,
                      parametr_option="cashback")
    time.sleep(conts_time)
    with allure.step(f"Проверка суммы тразакций из скрипта с суммой в талблице "):
        conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                password=config["DATABASE"]["password"],
                                host=config["DATABASE"]["host"])
        cursor = conn.cursor()

        postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
        cursor.execute(postgreSQL_select_Query)
        ft_records = cursor.fetchall()
        sum_1 = 0
        for i in range(0, 6):
            srtingIntable = ft_records[i]
            sum_1 += srtingIntable[2]

        conn.commit()
        print('Сумма в таблице=', sum_1)
        print('Сумма по тразакциям=', g.get_sum_2())

        assert sum_1 == g.get_sum_2()


def test_checkClickHouseSum_0():
    with allure.step(f"Кэшбэк-рубли Проверка вознаграждение по операциям = 1%"):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "5811", "5811"],
                          z_list=[10000, 10000, 10000, 10000, 10000, 10000], currency=810, parametr_option="cashback")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 500


def test_checkClickHouseSum_1():
    with allure.step(f"Кэшбэк-рубли Максимальная ежемесячная сумма начислений = 5 000 кэшбэк-руб"):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "5811", "5811"],
                          z_list=[1000000000, 100000000, 100000000, 10000, 100000000, 100000000], currency=810,
                          parametr_option="cashback")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 500000


def test_checkClickHouseSum_2():
    with allure.step(f"Кэшбэк-рубли проверка вознаграждение по операциям = 1% и Вознаграждение по категории "
                     f"«Развлечения» = 2,5%."):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "7832", "7832"],
                          z_list=[100000, 100000, 100000, 0, 100000, 0], currency=810,
                          parametr_option="cashback")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 5500


def test_checkClickHouseSum_3():
    with allure.step(f"Кэшбэк-рубли проверка вознаграждение по операциям = 1% и Вознаграждение по категории "
                     f"«Развлечения» = 2,5% ; +Повышенный кэшбэк начисляется только на покупки, совершенные в выбранной "
                     f"любимой категории, содержащей 30% от общего оборота, если оборот в повышенной категории свыше "
                     f"30%, то на этот оборот начисляется 1%. "):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "7832", "7832"],
                          z_list=[700000, 0, 0, 0, 300000, 0], currency=810,
                          parametr_option="cashback")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 14500


def test_checkClickHouseSum_4():
    with allure.step(f"Универсальные мили Максимальная ежемесячная сумма начислений = 15000 миль"):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "5811", "5811"],
                          z_list=[1000000000, 100000000, 100000000, 10000, 100000000, 100000000], currency=810,
                          parametr_option="mili")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 1500000


def test_checkClickHouseSum_5():
    with allure.step(f"Универсальные мили  проверка вознаграждение по операциям = 1% и Вознаграждение по категории "
                     f"«Развлечения» = 2,5%."):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "7832", "7832"],
                          z_list=[100000, 100000, 100000, 0, 100000, 0], currency=810,
                          parametr_option="mili")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 5500


def test_checkClickHouseSum_6():
    with allure.step(f"Универсальные мили проверка вознаграждение по операциям = 1% и Вознаграждение по категории "
                     f"«Развлечения» = 2,5%; +Повышенный кэшбэк начисляется только на покупки, совершенные в выбранной "
                     f"любимой категории, содержащей 30% от общего оборота, если оборот в повышенной категории свыше "
                     f"30%, то на этот оборот начисляется 1%. "):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "7832", "7832"],
                          z_list=[700000, 0, 0, 0, 300000, 0], currency=810,
                          parametr_option="mili")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 14500


def test_checkClickHouseSum_7():
    with allure.step(f"Бонусы партнерских программ (кроме Пятерочка и Перекресток) S7 ; Максимальная ежемесячная "
                     f"сумма начислений = 5 000 бонусов"):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "5811", "5811"],
                          z_list=[1000000000, 100000000, 100000000, 10000, 100000000, 100000000], currency=810,
                          parametr_option="s7")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 500000


def test_checkClickHouseSum_8():
    with allure.step(f"Бонусы партнерских программ (кроме Пятерочка и Перекресток) S7; Проверка "
                     f"вознаграждение по операциям = 1% и Вознаграждение по категории "
                     f"«Развлечения» = 2,5%."):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "7832", "7832"],
                          z_list=[100000, 100000, 100000, 0, 100000, 0], currency=810,
                          parametr_option="s7")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 5500


def test_checkClickHouseSum_9():
    with allure.step(f"Бонусы партнерских программ (кроме Пятерочка и Перекресток) S7 ; Проверка вознаграждение по "
                     f"операциям = 1% и Вознаграждение по категории "
                     f"«Развлечения» = 2,5%;+Повышенный кэшбэк начисляется только на покупки, совершенные в выбранной "
                     f"любимой категории, содержащей 30% от общего оборота, если оборот в повышенной категории свыше "
                     f"30%, то на этот оборот начисляется 1%. "):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "7832", "7832"],
                          z_list=[700000, 0, 0, 0, 300000, 0], currency=810,
                          parametr_option="s7")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 14500


def test_checkClickHouseSum_10():
    with allure.step(f"Бонусы партнерских программ (Пятерочка и Перекресток) ; Максимальная ежемесячная "
                     f"сумма начислений = 50 000 бонусов"):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "5811", "5811"],
                          z_list=[1000000000, 100000000, 100000000, 10000, 100000000, 100000000], currency=810,
                          parametr_option="perekrestok")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 5000000


def test_checkClickHouseSum_11():
    with allure.step(f"Бонусы партнерских программ (Пятерочка и Перекресток);Проверка "
                     f"вознаграждение по операциям = 10% и Вознаграждение по категории "
                     f"«Развлечения» = 25%."):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "7832", "7832"],
                          z_list=[100000, 100000, 100000, 0, 100000, 0], currency=810,
                          parametr_option="perekrestok")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 55000


def test_checkClickHouseSum_12():
    with allure.step(f"Бонусы партнерских программ (Пятерочка и Перекресток) ; Проверка вознаграждение по "
                     f"операциям = 10% и Вознаграждение по категории "
                     f"«Развлечения» = 25%+Повышенный кэшбэк начисляется только на покупки, совершенные в выбранной "
                     f"любимой категории, содержащей 30% от общего оборота, если оборот в повышенной категории свыше "
                     f"30%, то на этот оборот начисляется 1%. "):
        g = GeneratorUser(mcc_list=["4111", "5811", "9950", "6050", "7832", "7832"],
                          z_list=[700000, 0, 0, 0, 300000, 0], currency=810,
                          parametr_option="perekrestok")
        time.sleep(conts_time)

        for n in range(0, 6):
            with allure.step(f"Проверка статуса Delivered в строке c номером '{n}'"):
                conn = psycopg2.connect(dbname=config["DATABASE"]["dbname"], user=config["DATABASE"]["user"],
                                        password=config["DATABASE"]["password"],
                                        host=config["DATABASE"]["host"])
                cursor = conn.cursor()

                postgreSQL_select_Query = f"select * from ftype_writer.ftype_tf where card_id = '{g.get_cardId()}'"
                cursor.execute(postgreSQL_select_Query)
                ft_records = cursor.fetchall()
                srtingIntable = ft_records[n]
                status = srtingIntable[20]
                print(status)
                assert status == True

    with allure.step(f"Проверка суммы ClickHouse "):
        client = Client(host=config["Clickhouse"]["host"], user=config["Clickhouse"]["user"],
                        password=config["Clickhouse"]["password"])
        clickhouse_select_Query = f"SELECT * FROM {x} WHERE profile_id = '{g.get_profileId()}'"
        clickhouse_records = client.execute(clickhouse_select_Query)

        sum_3 = 0
        for t in range(len(clickhouse_records)):
            srtingIntable_2 = clickhouse_records[t]
            sum_3 += srtingIntable_2[3]

        print('сумма  в ClickHouse = ', sum_3)
        assert sum_3 == 145000
