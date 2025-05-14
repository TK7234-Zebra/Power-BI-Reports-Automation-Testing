import argparse
import yaml
import os
import pandas as pd
import time
from azure.identity import InteractiveBrowserCredential
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import ssl
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
from urllib.error import URLError
import base64            
from collections import defaultdict

EDGE_BASE_PROFILE_PATH = r"C:\Users\TK7234\AppData\Local\Microsoft\Edge\User Data"

def load_config(config_file):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

def send_email(to_email, subject, content, attachment_paths=None, max_retries=3, retry_delay=5):
    print("inside email")
    message = Mail(
    from_email='zedl_operations_nonprod@zebra.com',
    to_emails=to_email,
    subject=subject,
    html_content=content
    )
    if attachment_paths:
        for path in attachment_paths:
            if not os.path.exists(path):
                continue
            with open(path, 'rb') as f:
                data = f.read()
                encoded = base64.b64encode(data).decode()
                attachment = Attachment(
                    FileContent(encoded),
                    FileName(os.path.basename(path)),
                    FileType('image/png'),
                    Disposition('attachment')
                )
                message.attachment = attachment
    attempt = 0
    while attempt < max_retries:
        try:
            context = ssl.create_default_context()
            context.verify_flags &= ~ssl.VERIFY_X509_STRICT
            ssl._create_default_https_context = lambda: context
            sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
            response = sg.send(message)
            if response.status_code in (200, 202):
                print(f"Email sent successfully on attempt {attempt + 1}")
                return 
            print(f"Unexpected status code {response.status_code}, retrying...")
        except URLError as e:
            print(f"URLError on attempt {attempt + 1}: {e.reason}")
        except Exception as e:
            print(f"Exception on attempt {attempt + 1}: {str(e)}")
        attempt += 1
        if attempt < max_retries:
            time.sleep(retry_delay)
    print("Failed to send email after multiple attempts.")

class PowerBIReportProbe:
    def __init__(self, profile_suffix):
        self.profile_suffix = profile_suffix
        self.results = [["area", "report_name", "dataset_name", "url_report", "url_page", "page_nr", "has_error"]]
        self.has_found_any_errors = "no error"
        self.screenshots_dir = "Default"
        self.driver = None
        self.token = None
        self.powerBIBaseUrl = "https://api.powerbi.com/v1.0/myorg"
        self.api = "https://analysis.windows.net/powerbi/api/.default"
    
    def _authenticate(self):
        auth = InteractiveBrowserCredential()
        self.token = auth.get_token(self.api).token
        if self.token:
            print("Authenticated")

    def init_selenium_driver_edge(self):
        options = webdriver.EdgeOptions()
        profile_dir = os.path.join(EDGE_BASE_PROFILE_PATH, f"AutoProfile_{self.profile_suffix}")
        options.add_argument(f"--user-data-dir={profile_dir}")
        options.add_argument("--start-maximized")
        self.driver = webdriver.Edge(options=options)
        time.sleep(2)

    def get_report_page_url(self, report_base_url, page_number=None):
        if not page_number or page_number == 1 or page_number == 0:
            url = report_base_url + "/ReportSection"
        else:
            url = report_base_url + f"/ReportSection{page_number}"
        return url

    def get_report_page_id(self, url) -> str:
        sectionId = ""
        if "ReportSection" in url:
            sectionId = url.split("ReportSection")[1].split("?")[0]
        return sectionId

    def load_report_page_by_url(self, url, screenshot_name=None):
        print(f"loading url: {url}")
        try:
            self.driver.get(url)
        except Exception as e:
            print(f"Failed to load URL: {url}")
            print(f"Current URL: {self.driver.current_url}")
            print(f"Driver state: {self.driver.session_id}")
            raise e

        WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "pbi-overlay-container"))
        )

        try:
            pageNavBtn = WebDriverWait(self.driver, 1).until(
                EC.element_to_be_clickable((By.ID, "pageNavBtn"))
            )
            if pageNavBtn:
                pageNavBtn.click()
                print("Pages expanded")
        except:
            print("Page Expand Button not present.")
        
        print("Loaded Page")
        time.sleep(15)

        screenshot_path = None
        if screenshot_name:
            screenshots_dir = self.screenshots_dir
            os.makedirs(screenshots_dir, exist_ok=True)
            screenshot_path = os.path.join(screenshots_dir, screenshot_name)
            self.driver.save_screenshot(screenshot_path)
        return screenshot_path

    def has_report_page_error_visuals(self, seconds=10) -> bool:
        print(f"Checking for visuals with errors in {self.driver.current_url}")
        try:
            WebDriverWait(self.driver, seconds).until(
                EC.presence_of_element_located((By.TAG_NAME, "canvas-visual-error-overlay"))
            )
            self.has_found_any_errors = "error"
            return "error"
        except:
            print("no errors in visuals found")
            return "no error"

    def close_open_reports(self):
        try:
            close_buttons = self.driver.find_elements(By.XPATH, "//button[contains(@class, 'close-button')]")
            for button in close_buttons:
                try:
                    button.click()
                    print("Closed an open report")
                except Exception as e:
                    print(f"Failed to click close button: {e}")
        except Exception as e:
            print(f"An error occurred while trying to find close buttons: {e}")

    def get_report_all_pages(self, area, report_name, report_base_url, dataset_name=""):
        try:
            self.load_report_page_by_url(report_base_url)
            time.sleep(10)
            start_time = time.time() 
            try:
                mat_action_list = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//mat-action-list[@data-testid='pages-navigation-list']"))
                )
                buttons = mat_action_list.find_elements(By.TAG_NAME, "button")
            except Exception as e:
                buttons = [0] 
            current_page_number = 1
            report_pages_count = len(buttons)
            processed_reports_count = 0

            for button in buttons:
                try:
                    if type(button) != int:
                        try:
                            button.click()
                            print(f"Navigated to page {current_page_number}")
                        except Exception as e:
                            print(f"Failed to click on page button {current_page_number}: {e}")
                            continue

                    time.sleep(1)
                    try:
                        spans = self.driver.find_elements(By.CSS_SELECTOR, "span.textRun")
                        span_texts = [span.text.strip().lower() for span in spans if span.text.strip()]
                        # print(f"Span texts found: {span_texts}")

                        skip_keywords = ["home page", "navigation"]  # Add more keywords if needed
                        if any(skip_word in text for skip_word in skip_keywords for text in span_texts):
                            print(f"Skipping page {current_page_number} due to skip keyword match.")
                            current_page_number += 1
                            continue
                    except Exception as e:
                        print(f"Error checking for skip keywords on page {current_page_number}: {e}")

                    screenshot_name = f"{report_name.replace(' ', '_')}_page_{current_page_number}.png"
                    screenshots_dir = self.screenshots_dir
                    os.makedirs(screenshots_dir, exist_ok=True)
                    screenshot_path = os.path.join(screenshots_dir, screenshot_name)

                    try:
                        has_report_page_errors = self.has_report_page_error_visuals()
                        if has_report_page_errors == "error":
                            time.sleep(15)
                            self.driver.save_screenshot(screenshot_path)
                            end_time = time.time()
                            time_taken = end_time - start_time

                            report_page_url = self.driver.current_url
                            self.log_results(
                                area=area,
                                report_name=report_name,
                                dataset_name=dataset_name,
                                report_base_url=report_base_url,
                                url=report_page_url,
                                report_page_number=f"{current_page_number}/{report_pages_count}",
                                has_report_page_errors=has_report_page_errors,
                                screenshot_path=os.path.relpath(screenshot_path),
                                time_taken_seconds=time_taken
                            )
                            print(f"Error found on page {current_page_number}. Screenshot taken. Skipping to next page.")
                            current_page_number += 1
                            continue
                    except Exception as e:
                        print(f"Error checking visuals on page {current_page_number}: {e}")
                        has_report_page_errors = "check_failed"

                    try:
                        WebDriverWait(self.driver, 180).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "mid-viewport"))
                        )
                        print("Page fully loaded with mid-viewport detected.")
                    except Exception as e:
                        print(f"Timeout waiting for mid-viewport:")

                    report_page_url = self.driver.current_url
                    try:
                        has_report_page_errors = self.has_report_page_error_visuals()
                    except Exception as e:
                        print(f"Error checking visuals on page {current_page_number}: {e}")
                        has_report_page_errors = "check_failed"
                    try:
                        self.driver.save_screenshot(screenshot_path)
                    except Exception as e:
                        print(f"Failed to save screenshot: {e}")
                        screenshot_path = "N/A"

                    end_time = time.time()  
                    time_taken = end_time - start_time  

                    self.log_results(
                        area = area,
                        report_name=report_name,
                        dataset_name=dataset_name,
                        report_base_url=report_base_url,
                        url=report_page_url,
                        report_page_number=f"{current_page_number}/{report_pages_count}",
                        has_report_page_errors=has_report_page_errors,
                        screenshot_path=os.path.relpath(screenshot_path) if screenshot_path != "N/A" else "N/A",
                        time_taken_seconds=time_taken
                    )

                    print(f"Processing time for page {current_page_number}: {time_taken:.2f} seconds")
                    print(f"Page URL: {report_page_url}")
                    print(f"Page {current_page_number}/{report_pages_count} in report")
                    print(f"Has page errors: {has_report_page_errors}")
                    if has_report_page_errors == "error":
                        self.has_found_any_errors = "error"
                    current_page_number += 1
                    processed_reports_count += 1
                    if processed_reports_count % 5 == 0:
                        try:
                            print("Closing open reports...")
                            self.close_open_reports()
                        except Exception as e:
                            print(f"Failed to close open reports: {e}")
                except Exception as e:
                    print(f"Unexpected error on report page {current_page_number}: {e}")
                    current_page_number += 1
                    continue 
        except Exception as e:
            print(f"Fatal error while processing report '{report_name}': {e}")
            screenshot_path = os.path.join(self.screenshots_dir, f"{report_name.replace(' ', '_')}_error.png")
            try:
                time.sleep(15)
                self.driver.save_screenshot(screenshot_path)
            except:
                screenshot_path = "N/A"
            self.log_results(area, report_name, dataset_name, report_base_url, report_base_url, "error", "fatal_error", screenshot_path, 0)

    def log_results(self, area, report_name, dataset_name, report_base_url, url, report_page_number, has_report_page_errors, screenshot_path=None, time_taken_seconds=0):
        self.results.append([
            area, 
            report_name,
            dataset_name,
            report_base_url,
            url,
            report_page_number,
            has_report_page_errors,
            screenshot_path or "N/A",
            f"{time_taken_seconds:.2f}"
        ])
    
    def show_results(self):
        df = pd.DataFrame.from_records(self.results)
        print(df)
        df.columns = df.iloc[0]
        df = df[1:]
        df["url_report"] = df["url_report"].apply(lambda x: f'<a href="{x}" target="_blank">link to report</a>')
        df["url_page"] = df["url_page"].apply(lambda x: f'<a href="{x}" target="_blank">link to page</a>')
        
        # Embed the screenshot as an image in the HTML
        df["screenshot_path"] = df["screenshot_path"].apply(
            lambda x: f'<a href="{x}" target="_blank"><img src="{x}" alt="Screenshot" style="width:150px;height:auto;"></a>' if x != "N/A" else "N/A"
        )
        html_file_path = os.path.join(os.getcwd(), "result.html")
        html_table = df.to_html(index=False, escape=False, render_links=True)
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Report Results</title>
            <link rel="stylesheet" href="style.css">
        </head>
        <body>
            <h1>Power BI Report Validation Results</h1>
            {html_table}
        </body>
        </html>
        """
        with open(html_file_path, "w", encoding="utf-8") as file:
            file.write(html_content)
        print(f"Results have been saved to {html_file_path}. Open this file to view the results.")

    def quit_driver(self):
        self.close_open_reports()
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"Driver quit error: {e}")

def run_reports_in_parallel(excel_files):
    merged_results = defaultdict(dict)
    all_keys = set()
    any_errors = "no error" 
    all_rows = []
    probes = []
    probe = PowerBIReportProbe("auth")
    # probe._authenticate()
    # probe.quit_driver()
    def wrapped_process_reports(excel_file, profile_suffix):
        probe = PowerBIReportProbe(profile_suffix)
        probe.init_selenium_driver_edge()
        probes.append(probe)
        df = pd.read_excel(excel_file)
        for index, report in df.iterrows():
            area = os.path.splitext(excel_file)[0].lower()
            probe.get_report_all_pages(area, report["PBI Report Name"], report["PBI Link"], report["PBI Dataset Name"])
            probe.close_open_reports()
        results_with_instance = []
        for row in probe.results[1:]:  # Skip header row
            results_with_instance.append(row)
        return results_with_instance, probe.has_found_any_errors

    with ThreadPoolExecutor(max_workers=MAX_INSTANCES) as executor:
        futures = [executor.submit(wrapped_process_reports, excel_file, i)
                   for i, excel_file in enumerate(excel_files)]
        for future in futures:
            results, has_error = future.result()
            all_rows.extend(results)
            if has_error:
                any_errors = "error"  
            for row in results:
                key = (row[2], row[3], row[4])  # (url_report, url_page, page_nr)
                instance = row[6]
                merged_results[key][instance] = row[5]
                all_keys.add(key)

    for probe in probes:
        try:
            probe.quit_driver()
        except Exception as e:
            print(f"Failed to quit driver: {e}")
    headers = ["area", "report_name", "dataset_name", "url_report", "url_page", "page_num", "has_report_page_errors", "screenshot_path"]
    results_all = [headers]
    for key in sorted(all_keys, key=lambda x:str(x)):
        base_row = next(r for r in all_rows if (r[2], r[3], r[4]) == key)
        row = base_row[:-1]
        results_all.append(row)
    return results_all, any_errors

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='tenants.yaml')
    parser.add_argument('-t', '--tenant', default='DEFAULT')
    args = parser.parse_args()

    config = load_config(args.config)
    profile_name = config.get(args.tenant)

    EXCEL_TO_EMAIL_MAP = {
        "finance": "tushar.kumarchopra@zebra.com",
        "sales": ["tushar.kumarchopra@zebra.com", "vaishali.dn@zebra.com"],
        "gscr": "yeshwant.sp@zebra.com",
        "services": "tushar.kumarchopra@zebra.com"
    }

    excel_files = [f for f in os.listdir() if f.endswith(".xlsx") and not f.startswith("~$")]
    MAX_INSTANCES = len(excel_files)

    all_results, has_errors = run_reports_in_parallel(excel_files)

    temp_probe = PowerBIReportProbe(profile_suffix="merged")
    temp_probe.results = all_results
    temp_probe.show_results()

    for file in excel_files:
        area_base = os.path.splitext(file)[0]
        area = area_base.split()[0].lower()
        print(f"Area:{area}")
        email_recipient = EXCEL_TO_EMAIL_MAP.get(area)
        if isinstance(email_recipient, list):
            email_recipient=",".join(email_recipient)
        file_errors = [
            row for row in all_results[1:]
            if (os.path.splitext(row[0])[0].split()[0].lower()) == area and str(row[6]).strip() == "error"
        ]
        print(file_errors)

        # Build summary stats
        summary_stats = defaultdict(lambda: {"total": 0, "errors": 0})
        for row in all_results[1:]:
            file_base = os.path.splitext(row[0])[0]
            filename = file_base.split()[0].lower()
            print(f"file:{filename}")
            if filename != area:
                continue
            dataset = row[2]
            status = str(row[6]).strip().lower()
            summary_stats[dataset]["total"] += 1
            if status == "error":
                summary_stats[dataset]["errors"] += 1
        
        print(summary_stats)

        # Start email content with summary table
        email_content = f"""
        <h2>Summary of report check results for area: {area.upper()}</h2>
        <table border='1' style='border-collapse: collapse;'>
            <tr>
                <th style='padding: 8px;'>Semantic Model</th>
                <th style='padding: 8px;'>Total Reports</th>
                <th style='padding: 8px;'>Errors</th>
                <th style='padding: 8px;'>Successful</th>
            </tr>
        """

        for dataset, stats in summary_stats.items():
            successful = stats["total"] - stats["errors"]
            email_content += f"""
            <tr>
                <td style='padding: 8px;'>{dataset}</td>
                <td style='padding: 8px;'>{stats['total']}</td>
                <td style='padding: 8px;'>{stats['errors']}</td>
                <td style='padding: 8px;'>{successful}</td>
            </tr>
            """

        email_content += "</table>"

        if file_errors:
            email_content += "<h3>Errored Reports below:</h3>"
            attachment_paths = []
            for row in file_errors:
                report_name = row[1]
                dataset = row[2]
                screenshot_path = row[7]
                email_content += f"<p>Report: {report_name} <br>Dataset: {dataset}</p>"
                if screenshot_path != "N/A" and os.path.exists(screenshot_path):
                    attachment_paths.append(screenshot_path)
                else:
                    print("No screenshot for:", report_name)

        send_email(
            to_email=email_recipient,
            subject=f"Power BI Errors Detected - {area.capitalize()}",
            content=email_content,
            attachment_paths=attachment_paths
        )
