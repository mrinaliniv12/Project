def extract_all_node(state: dict) -> dict:
    start_time = time.time()
    batch_size = 4
    max_images = 20
    image_folder = state["input_path"]
    output_folder = "/Workspace/Users/mrinalini.vettri@fisglobal.com/Invoice Sense/OutputData/json_test"

    analyzer = InvoiceAnalyzer(image_folder, invoice_prompt)

    # Get image files to process
    image_files = get_image_files(image_folder, max_images)

    # Batch process and save JSONs
    process_images(image_files, batch_size)

    # Read all processed JSON files
    json_files = get_json_files(output_folder)
    all_data = []
    tabular_count = 0
    invoice_count = 0
    itemise_count = 0

    for json_file in json_files:
        with open(json_file, 'r') as file:
            try:
                json_data = json.load(file)
                if not isinstance(json_data, dict):
                    continue
            except Exception:
                continue

            result = analyzer.extract_all(json_data)
            all_data.append(result)
            if result.get("items_table"):
                tabular_count += 1
                itemise_count += 1
            if analyzer.extract_invoice_amount(json_data) is not None:
                invoice_count += 1

    total_time = time.time() - start_time
    total_count = len(all_data)
    avg_time = round(total_time / total_count, 2) if total_count else 0
    state["invoice_data"] = all_data
    state["total_invoices_processed"] = total_count
    state["average_time_per_invoice"] = avg_time
    state["node_details"] = [
        {"node": "tabular", "processed_count": tabular_count, "average_time": avg_time},
        {"node": "invoice_amount", "processed_count": invoice_count, "average_time": avg_time},
        {"node": "itemise", "processed_count": itemise_count, "average_time": avg_time},
    ]
    return state

def extract_invoice_amount_node(state: dict) -> dict:
    start_time = time.time()
    batch_size = 4
    max_images = 20
    image_folder = state["input_path"]
    output_folder = "/Workspace/Users/mrinalini.vettri@fisglobal.com/Invoice Sense/OutputData/json_test"

    analyzer = InvoiceAnalyzer(image_folder, invoice_prompt)

    # Get image files to process
    image_files = get_image_files(image_folder, max_images)

    # Batch process and save JSONs
    process_images(image_files, batch_size)

    # Read all processed JSON files
    json_files = get_json_files(output_folder)
    invoice_amounts = []

    for json_file in json_files:
        with open(json_file, 'r') as file:
            try:
                json_data = json.load(file)
                if not isinstance(json_data, dict):
                    continue
            except Exception:
                continue

            amount = analyzer.extract_invoice_amount(json_data)
            if amount is not None:
                invoice_amounts.append(amount)

    total_time = time.time() - start_time
    total_count = len(invoice_amounts)
    avg_time = round(total_time / total_count, 2) if total_count else 0
    state["invoice_data"] = invoice_amounts
    state["total_invoices_processed"] = total_count
    state["average_time_per_invoice"] = avg_time
    return state

def extract_itemize_node(state: dict) -> dict:
    start_time = time.time()
    batch_size = 4
    max_images = 20
    image_folder = state["input_path"]
    output_folder = "/Workspace/Users/mrinalini.vettri@fisglobal.com/Invoice Sense/OutputData/json_test"

    analyzer = InvoiceAnalyzer(image_folder, invoice_prompt)

    # Get image files to process
    image_files = get_image_files(image_folder, max_images)

    # Batch process and save JSONs
    process_images(image_files, batch_size)

    # Read all processed JSON files
    json_files = get_json_files(output_folder)
    itemized_data = []

    for json_file in json_files:
        with open(json_file, 'r') as file:
            try:
                json_data = json.load(file)
                if not isinstance(json_data, dict):
                    continue
            except Exception:
                continue

            itemized = analyzer.extract_itemize(json_data)
            if itemized:
                itemized_data.append(itemized)

    total_time = time.time() - start_time
    total_count = len(itemized_data)
    avg_time = round(total_time / total_count, 2) if total_count else 0
    state["invoice_data"] = itemized_data
    state["total_invoices_processed"] = total_count
    state["average_time_per_invoice"] = avg_time
    return state
