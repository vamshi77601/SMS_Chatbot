import psycopg2
import requests
import json

# PostgreSQL connection details – update these as necessary.
DB_HOST = "localhost"
DB_NAME = "phi3db"
DB_USER = "postgres"
DB_PASSWORD = "JUN@2001"

# Ollama API setup
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "phi3"

def fetch_sms_data(msisdn=None, request_time_start=None, request_time_end=None, div_id=None):
    """
    Fetch SMS details from the database by unioning two queries:
      - Query 1 joins "SMS" and "DLR_S"
      - Query 2 joins "SMS_V6" and "DLR_SV6"
     
   
    Returns a formatted string with headers and table-like rows.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Build the WHERE clause for Query 1 (for "SMS")
        conditions_q1 = []
        if msisdn:
            conditions_q1.append(f'"SMS"."MSISDN" = \'{msisdn}\'')
        if request_time_start and request_time_end:
            conditions_q1.append(
                f"date_trunc('day', \"SMS\".\"REQUEST_TIME\")::date BETWEEN '{request_time_start}'::date AND '{request_time_end}'::date"
            )
        elif request_time_start:
            conditions_q1.append(
                f"date_trunc('day', \"SMS\".\"REQUEST_TIME\")::date = '{request_time_start}'::date"
            )
        elif request_time_end:
            conditions_q1.append(
                f"date_trunc('day', \"SMS\".\"REQUEST_TIME\")::date = '{request_time_end}'::date"
            )
        if div_id:
            conditions_q1.append(f'"SMS"."DIV_ID" = \'{div_id}\'')
        where_clause_q1 = " AND ".join(conditions_q1) if conditions_q1 else "1=1"

        # Build the WHERE clause for Query 2 (for "SMS_V6")
        conditions_q2 = []
        if msisdn:
            conditions_q2.append(f'"SMS_V6"."MSISDN" = \'{msisdn}\'')
        if request_time_start and request_time_end:
            conditions_q2.append(
                f"date_trunc('day', \"SMS_V6\".\"REQUEST_TIME\")::date BETWEEN '{request_time_start}'::date AND '{request_time_end}'::date"
            )
        elif request_time_start:
            conditions_q2.append(
                f"date_trunc('day', \"SMS_V6\".\"REQUEST_TIME\")::date = '{request_time_start}'::date"
            )
        elif request_time_end:
            conditions_q2.append(
                f"date_trunc('day', \"SMS_V6\".\"REQUEST_TIME\")::date = '{request_time_end}'::date"
            )
        if div_id:
            conditions_q2.append(f'"SMS_V6"."DIV_ID" = \'{div_id}\'')
        where_clause_q2 = " AND ".join(conditions_q2) if conditions_q2 else "1=1"

        # Query 1: Join "SMS" and "DLR_S"
        query1 = f"""
        SELECT
            "SMS"."MSISDN",
            "SMS"."REQUEST_TIME",
            "SMS"."DIV_ID",
            "SMS"."MESSAGE_TEXT",
            "DLR_S"."STATUS",
            "DLR_S"."DELIVERY_TIME"
        FROM "SMS"
        JOIN "DLR_S" ON "SMS"."MSISDN" = "DLR_S"."MSISDN"
        WHERE {where_clause_q1}
        """

        # Query 2: Join "SMS_V6" and "DLR_SV6"
        query2 = f"""
        SELECT
            "SMS_V6"."MSISDN",
            "SMS_V6"."REQUEST_TIME",
            "SMS_V6"."DIV_ID",
            "SMS_V6"."MESSAGE_TEXT",
            "DLR_SV6"."STATUS",
            "DLR_SV6"."DELIVERY_TIME"
        FROM "SMS_V6"
        JOIN "DLR_SV6" ON "SMS_V6"."MSISDN" = "DLR_SV6"."MSISDN"
        WHERE {where_clause_q2}
        """

        # Combine both queries using UNION ALL.
        sql_query = f"({query1}) UNION ALL ({query2});"
        #print("Constructed SQL Query:\n", sql_query)  # Print query for validation

        cursor.execute(sql_query)
        result = cursor.fetchall()
        conn.close()

        # Format the results into a table-like string.
        header = "MSISDN | REQUEST_TIME | DIV_ID | MESSAGE_TEXT | STATUS | DELIVERY_TIME\n"
        formatted_data = header
        if not result:
            return "No matching records found for the Entered Details."
        for row in result:
            formatted_row = " | ".join([str(col) if col is not None else "NULL" for col in row])
            formatted_data += formatted_row + "\n"
        return formatted_data

    except Exception as e:
        return f"Error fetching SMS data: {e}"

def query_phi3_stream(prompt):
    """
    Sends a prompt to the Phi‑3 model via the Ollama API with streaming enabled.
    It collects the streamed JSON responses and returns them as a single string.
    """
    try:
        payload = {"model": MODEL, "prompt": prompt}
        response = requests.post(OLLAMA_URL, json=payload, stream=True)
        collected_response = ""
        for line in response.iter_lines():
            try:
                json_chunk = json.loads(line.decode('utf-8'))
                collected_response += json_chunk.get("response", "")
            except json.JSONDecodeError:
                continue  # Ignore malformed lines
        return collected_response.strip() or "No valid response received."
    except Exception as e:
        return f"Error communicating with Phi3: {e}"

def create_prompt(context, user_query):
    """
    Constructs the prompt for the Phi‑3 model.
    It includes a role instruction to answer exclusively based on the SMS data and to return a table.
    """
    role_instruction = (
        "You are a Database Query Assistant. Your task is to answer queries exclusively based on the SMS data provided below. "
        "Do not invent new data; only refer to the information given. Format your answer as a table with headers. "
        "If the question does not relate to the provided data, reply with: 'I can only answer questions related to the provided SMS data.'\n\n"
    )
    prompt = f"{role_instruction}{context}\nAnswer this query: {user_query}"
    return prompt

def main():
    print("Welcome to the SMS Chatbot! Type 'exit' at any time to quit.")

    # Main interactive loop comes first.
    while True:
        user_query = input("\nEnter your query about SMS data(Type 'exit' to quit): ").strip()
        if user_query.lower() == "exit":
            print("Exiting chatbot.")
            break
        if not any(keyword in user_query.lower() for keyword in ["sms", "mobile", "number"]):
            print("I can answer queries related to 'sms', 'mobile number' and 'Delivered status of SMS'. Please re-enter your query.")
            continue


        # Now retrieve filter details for the database query.
        # MSISDN is mandatory.
        while True:
            msisdn = input("\nEnter Mobile Number[Mandatory]: ").strip()
            if msisdn == "":
                print("MSISDN is mandatory. Please enter your Mobile Number.")
            else:
                break

        # Request Time range inputs (optional). You can leave either or both empty.
        request_time_start = input("Enter Start date(YYYY-MM-DD) : ").strip()
        request_time_end = input("Enter End date (YYYY-MM-DD) : ").strip()

        # Division ID is optional.
        div_id = input("Enter Division ID [Optional]: ").strip().upper()

        # Fetch the SMS data based on the provided filters.
        db_context = fetch_sms_data(
            msisdn,
            request_time_start if request_time_start else None,
            request_time_end if request_time_end else None,
            div_id if div_id else None
        )
        print("\nSMS Data:\n")
        #print(db_context)

        # Construct prompt for Phi‑3 using the retrieved database context.
        prompt = create_prompt(db_context, user_query)
        phi3_response = query_phi3_stream(prompt)
        print("\nPhi‑3 Response:\n")
        print(phi3_response)

if __name__ == "__main__":
    main()

