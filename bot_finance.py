import logging
from typing import Dict
from datetime import datetime, timedelta
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from db_utils import insert, sql_select, sql_query
import tabulate
import pandas as pd
import asyncio
from toke import TOKEN
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    PicklePersistence,
    filters,
    CallbackContext
)

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

START, CHOICE, DATE, AMOUNT, ACQUIRING, CARD_NUMBER, CATEGORY, VALID, DETAILS = range(
    9)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.message.from_user.id
    if int(user_id) == 745584051:
        context.user_data['replay_keyboard'] = [
            ['Трата', 'Пополнение', 'Переводы между счетами']]
        await update.message.reply_text(
            "Привет!",
            reply_markup=ReplyKeyboardMarkup(
                context.user_data.get('replay_keyboard'), one_time_keyboard=True
            ),
        )
        return CHOICE
    else:
        return ConversationHandler.END


async def choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    if text == "Трата":
        context.user_data['operation_type'] = 1
        ReplyKeyboardRemove()
        reply_keyboard = [['Дата сегодня', 'Вернуться назад']]
        await update.message.reply_text(
            f"Выберите дату транзакции - трата",
            reply_markup=ReplyKeyboardMarkup(
                reply_keyboard, one_time_keyboard=True,
                input_field_placeholder="Напиши дату, или выбери пункт - Дата сегодня"
            ),
        )
        return DATE
    elif text == 'Пополнение':
        context.user_data['operation_type'] = 2
        ReplyKeyboardRemove()
        reply_keyboard = [['Дата сегодня', 'Вернуться назад']]
        await update.message.reply_text(
            f"Выберите дату транзакции - пополнение",
            reply_markup=ReplyKeyboardMarkup(
                reply_keyboard, one_time_keyboard=True,
                input_field_placeholder="Напиши дату, или выбери пункт - Дата сегодня"
            ),
        )
        return DATE
    elif text == 'Переводы между счетами':
        context.user_data['operation_type'] = 3
        ReplyKeyboardRemove()
        reply_keyboard = [['Дата сегодня', 'Вернуться назад']]
        await update.message.reply_text(
            f"Выберите дату транзакции - перевод между счетами",
            reply_markup=ReplyKeyboardMarkup(
                reply_keyboard, one_time_keyboard=True,
                input_field_placeholder="Напиши дату, или выбери пункт - Дата сегодня"
            ),
        )
        return DATE


async def date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    if text == "Дата сегодня":
        context.user_data["user_date"] = datetime.now().strftime('%Y-%m-%d')
        operation = context.user_data.get('operation_type')
        if operation == 1:
            query = '''select * from my_finance.bank_account'''
            message = sql_select(database_name='main_db', sql=query)
            message.set_index('id', inplace=True)
            await update.message.reply_text(
                f"{message.to_markdown()}")
            await update.message.reply_text(
                f"Напишите id карты списания")
            return CARD_NUMBER
        elif operation == 2:
            query = '''select * from my_finance.bank_account'''
            message = sql_select(database_name='main_db', sql=query)
            message.set_index('id', inplace=True)
            await update.message.reply_text(
                f"{message.to_markdown()}")
            await update.message.reply_text(
                f"Напишите id карты на которую произошло пополнение")
            return CARD_NUMBER
        else:
            query = '''select * from my_finance.bank_account'''
            message = sql_select(database_name='main_db', sql=query)
            message.set_index('id', inplace=True)
            await update.message.reply_text(
                f"{message.to_markdown()}")
            await update.message.reply_text(
                f"Сначала укажите айди карты списания")
            return CARD_NUMBER
    elif text == 'Вернуться назад':
        await update.message.reply_text(
            'Выберите далнейшие действия',
            reply_markup=ReplyKeyboardMarkup(
                context.user_data.get('replay_keyboard'), one_time_keyboard=True
            ))
        return CHOICE
    else:
        if text > datetime.now().strftime('%Y-%m-%d'):
            reply_keyboard = [['Дата сегодня', 'Вернуться назад']]
            await update.message.reply_text(
                f"Вы ввели дату, которая еще не наступила выберите - Дата сегодня или напишите свой день - формат - '%Y-%m-%d', если вы не будете использовать нужный формат, то оне не пустит дальше",
                reply_markup=ReplyKeyboardMarkup(
                    reply_keyboard, one_time_keyboard=False,
                    input_field_placeholder="Напиши дату, или выбери пункт - Дата сегодня"
                ),
            )
            return DATE
        else:
            context.user_data["user_date"] = text


async def card_number(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    context.user_data["first_card"] = text
    operation = context.user_data.get('operation_type')
    if operation == 3:
        await update.message.reply_text(f"Напишите карту получения")
        return ACQUIRING
    else:
        await update.message.reply_text(f"Напишите сумму операции")
        return AMOUNT


async def acquiring(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    context.user_data["second_card"] = text
    await update.message.reply_text(f"Напишите сумму операции")
    return AMOUNT


async def amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    context.user_data["amount"] = text
    operation = context.user_data.get('operation_type')
    if operation == 1:
        query = '''select * from my_finance.category'''
        message = sql_select(database_name='main_db', sql=query)
        message.set_index('id', inplace=True)
        await update.message.reply_text(f"Напишите категорию операции и напишите id этой категории или напишите свою")
        await update.message.reply_text(f"{message.to_markdown()}")
        return CATEGORY
    elif operation == 2:
        reply_keyboard = [["Да", "Нет"]]
        df_valid = sql_select(database_name='main_db',
                              sql=f'''select * from my_finance.bank_account 
                              where id = {context.user_data.get('first_card')}''')
        total_balance = int(df_valid['balance'].iloc[0]) + \
            int(context.user_data.get('amount'))
        context.user_data["bank"] = df_valid['bank'].iloc[0]
        context.user_data["card_type"] = df_valid['card_type'].iloc[0]
        await update.message.reply_text(
            f'''Подтвердите операцию: пополнение на карту - {df_valid['bank'].iloc[0]}, с типом карты {df_valid['card_type'].iloc[0]} \
            на сумму {context.user_data.get('amount')}''')
        await update.message.reply_text(
            f'''Баланс до операции {df_valid['balance'].iloc[0]}, баланс после операции {total_balance}''',
            reply_markup=ReplyKeyboardMarkup(
                reply_keyboard, one_time_keyboard=True
            ))
        return VALID
    elif operation == 3:
        reply_keyboard = [["Да", "Нет"]]
        df_valid = sql_select(database_name='main_db',
                              sql=f'''select * from my_finance.bank_account 
                              where id = {context.user_data.get('first_card')}''')
        df_valid_2 = sql_select(database_name='main_db',
                                sql=f'''select * from my_finance.bank_account 
                              where id = {context.user_data.get('second_card')}''')
        context.user_data["bank"] = df_valid['bank'].iloc[0]
        context.user_data["bank_2"] = df_valid_2['bank'].iloc[0]
        context.user_data["card_type"] = df_valid['card_type'].iloc[0]
        context.user_data["card_type_2"] = df_valid_2['card_type'].iloc[0]
        total_balance_1 = int(df_valid['balance'].iloc[0]) - \
            int(context.user_data.get('amount'))
        total_balance_2 = int(df_valid_2['balance'].iloc[0]) + \
            int(context.user_data.get('amount'))
        await update.message.reply_text(
            f'''Подтвердите операцию: перевод с карты - {df_valid['bank'].iloc[0]}, с типом карты {df_valid['card_type'].iloc[0]} \
            на карту {df_valid_2['bank'].iloc[0]}, с типом карты {df_valid_2['card_type'].iloc[0]} 
            на сумму {context.user_data.get('amount')}''')
        await update.message.reply_text(
            f'''Баланс первой карты до снятия {df_valid['balance'].iloc[0]}, баланс после операции {total_balance_1}, а \
                Баланс второй карты до пополнения {df_valid_2['balance'].iloc[0]}, баланс после операции {total_balance_2}''',
            reply_markup=ReplyKeyboardMarkup(
                reply_keyboard, one_time_keyboard=True
            ))
        return VALID


async def category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    try:
        query = f'''select * from my_finance.category where id = {text}'''
        df_cat = sql_select(database_name='main_db', sql=query)
        context.user_data["category"] = df_cat['category'].iloc[0]
        await update.message.reply_text(f"Напишите детали транзакции по списанию")
        return DETAILS
    except Exception as E:
        print(E)
        try:
            int(text)
            await update.message.reply_text(f"Такой категории нет, введите иной id или свою категорию")
            return CATEGORY
        except Exception as E:
            print(E)
            dict_df = {'category': [text]}
            df_cat = pd.DataFrame(dict_df)
            insert(df=df_cat, database_name='main_db',
                   table_name='category', schema='my_finance')
            await update.message.reply_text(f"Напишите детали транзакции по списанию")
            return DETAILS


async def details(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    context.user_data["details"] = text
    reply_keyboard = [["Да", "Нет"]]
    df_valid = sql_select(database_name='main_db',
                          sql=f'''select * from my_finance.bank_account 
                              where id = {context.user_data.get('first_card')}''')
    context.user_data["bank"] = df_valid['bank'].iloc[0]
    context.user_data["card_type"] = df_valid['card_type'].iloc[0]
    await update.message.reply_text(f'''Подтвердите операцию: трата с карты - {df_valid['bank'].iloc[0]}, \
                                    с типом карты {df_valid['card_type'].iloc[0]} на сумму {context.user_data.get('amount')}''',
                                    reply_markup=ReplyKeyboardMarkup(
                                        reply_keyboard, one_time_keyboard=True
                                    ))
    return VALID


async def valid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    operation = context.user_data.get('operation_type')
    if update.message.text == "Да":
        if operation == 2:
            try:
                df_insert_popolnenie = {'date': [context.user_data.get("user_date")],
                                        'card_deposit': [context.user_data.get("amount")],
                                        'bank': [context.user_data.get("bank")],
                                        'card_type': [context.user_data.get("card_type_2")],
                                        'is_accounted': [0]}
                df_insert_popolnenie = pd.DataFrame(df_insert_popolnenie)
                insert(df=df_insert_popolnenie, database_name='main_db',
                       table_name='card_deposits', schema='my_finance')
            except Exception as E:
                print(E)
                print('first_insert')
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            query_1 = f'''update my_finance.bank_account 
            set balance = balance + {context.user_data.get("amount")}
            where id = {context.user_data.get('first_card')}'''
            query_2 = f'''update my_finance.card_deposits 
            set is_accounted = 1
            where is_accounted = 0'''
            try:
                sql_query(query_1)
            except Exception as E:
                print(E)
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            sql_query(query_2)
            await update.message.reply_text(
                'Все ходы записаны',
                reply_markup=ReplyKeyboardMarkup(
                    context.user_data.get('replay_keyboard'), one_time_keyboard=True
                ))
            return CHOICE
        elif operation == 3:
            df_insert_spisanie = {'date': [context.user_data.get("user_date")],
                                  'category': ['Переводы'],
                                  'details': ['Перевод с карты на карту'],
                                  'amount': [context.user_data.get("amount")],
                                  'card_type': [context.user_data.get("card_type")],
                                  'bank': [context.user_data.get("bank")],
                                  'is_accounted': [0]}
            df_insert_spisanie = pd.DataFrame(df_insert_spisanie)
            try:
                insert(df=df_insert_spisanie, database_name='main_db',
                       table_name='purchases', schema='my_finance')
            except Exception as E:
                print(E)
                print('Ошибка при списании')
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            df_insert_popolnenie = {'date': [context.user_data.get("user_date")],
                                    'card_deposit': [context.user_data.get("amount")],
                                    'bank': [context.user_data.get("bank")],
                                    'card_type': [context.user_data.get("card_type")],
                                    'is_accounted': [0]}
            df_insert_popolnenie = pd.DataFrame(df_insert_popolnenie)
            try:
                insert(df=df_insert_popolnenie, database_name='main_db',
                       table_name='card_deposits', schema='my_finance')
            except Exception as E:
                print(E)
                print('Ошибка при пополнении')
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            query_1 = f'''update my_finance.bank_account 
            set balance = balance - {context.user_data.get("amount")}
            where id = {context.user_data.get('first_card')}'''
            try:
                sql_query(query_1)
            except Exception as E:
                print('Error with spisanie')
                print(E)
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            query_2 = f'''update my_finance.purchases
            set is_accounted = 1
            where is_accounted = 0'''
            try:
                sql_query(query_2)
            except Exception as E:
                print('Error with my_finance.purchases')
                print(E)
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            query_3 = f'''update my_finance.bank_account 
            set balance = balance + {context.user_data.get("amount")}
            where id = {context.user_data.get('second_card')}'''
            try:
                sql_query(query_3)
            except Exception as E:
                print('Error with my_finance.bank_account +')
                print(E)
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            query_4 = f'''update my_finance.card_deposits
            set is_accounted = 1
            where is_accounted = 0'''
            try:
                sql_query(query_4)
            except Exception as E:
                print('Error with my_finance.card_deposits +')
                print(E)
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE

            await update.message.reply_text(
                'Все ходы записаны',
                reply_markup=ReplyKeyboardMarkup(
                    context.user_data.get('replay_keyboard'), one_time_keyboard=True
                ))
            return CHOICE
        else:
            df_insert_spisanie = {'date': [context.user_data.get("user_date")],
                                  'category': [context.user_data.get("category")],
                                  'details': [context.user_data.get("details")],
                                  'amount': [context.user_data.get("amount")],
                                  'card_type': [context.user_data.get("card_type")],
                                  'bank': [context.user_data.get("bank")],
                                  'is_accounted': [0]}
            df_insert_spisanie = pd.DataFrame(df_insert_spisanie)
            try:
                insert(df=df_insert_spisanie, database_name='main_db',
                       table_name='purchases', schema='my_finance')
            except Exception as E:
                print(E)
                print('Ошибка при списании')
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            query_1 = f'''update my_finance.bank_account 
            set balance = balance - {context.user_data.get("amount")}
            where id = {context.user_data.get('first_card')}'''
            try:
                sql_query(query_1)
            except Exception as E:
                print('Error with spisanie')
                print(E)
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            query_2 = f'''update my_finance.purchases
            set is_accounted = 1
            where is_accounted = 0'''
            try:
                sql_query(query_2)
            except Exception as E:
                print('Error with my_finance.purchases')
                print(E)
                await update.message.reply_text(f"Неудача, попробуйте снова")
                return CHOICE
            await update.message.reply_text(
                'Все ходы записаны',
                reply_markup=ReplyKeyboardMarkup(
                    context.user_data.get('replay_keyboard'), one_time_keyboard=True
                ))
            return CHOICE
    else:
        await update.message.reply_text(
            'Начинай заново',
            reply_markup=ReplyKeyboardMarkup(
                context.user_data.get('replay_keyboard'), one_time_keyboard=True
            ))
        return CHOICE


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.message.from_user
    logger.info("User %s canceled the conversation.", user.first_name)
    await update.message.reply_text(
        "Bye! I hope we can talk again some day.", reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


def main() -> None:
    """Run the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(
        TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            START: [MessageHandler(filters.TEXT & ~filters.COMMAND, start)],
            CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, choice)],
            DATE: [MessageHandler(filters.Regex('\d{4}-\d{2}-\d{2}|Дата сегодня|Вернуться назад'), date)],
            CARD_NUMBER: [MessageHandler(filters.TEXT & ~filters.COMMAND, card_number)],
            CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, category)],
            DETAILS: [MessageHandler(filters.TEXT & ~filters.COMMAND, details)],
            AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, amount)],
            ACQUIRING: [MessageHandler(filters.TEXT & ~filters.COMMAND, acquiring)],
            VALID: [MessageHandler(filters.Regex('Да|Нет'), valid)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(conv_handler)

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
