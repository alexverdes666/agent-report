"""
Main scraper script for the agent report website.
Uses Playwright for browser automation to extract data.
"""

import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright
import pandas as pd

from config import BASE_URL, BROWSER_CONFIG, SCRAPING_CONFIG, OUTPUT_CONFIG, AUTH_CONFIG


class AgentReportScraper:
    def __init__(self, target_year=None, target_month=None):
        self.scraped_data = []
        self.target_year = target_year
        self.target_month = target_month

    async def setup_browser(self, playwright):
        """Initialize browser with configuration."""
        browser = await playwright.chromium.launch(
            headless=BROWSER_CONFIG["headless"],
            args=BROWSER_CONFIG.get("args", [])
        )

        context = await browser.new_context(
            viewport=BROWSER_CONFIG["viewport"],
            user_agent=BROWSER_CONFIG["user_agent"]
        )

        page = await context.new_page()
        page.set_default_timeout(BROWSER_CONFIG["timeout"])

        return browser, page

    async def handle_authentication(self, page):
        """Handle login if credentials are provided."""
        if not AUTH_CONFIG["username"] or not AUTH_CONFIG["password"]:
            print("No credentials provided. Attempting to access without authentication...")
            return

        print("Attempting to login...")
        try:
            # Look for common login form elements
            username_selectors = [
                'input[type="text"]',
                'input[name="username"]',
                'input[name="email"]',
                'input[id="username"]',
                'input[id="email"]'
            ]

            password_selectors = [
                'input[type="password"]',
                'input[name="password"]',
                'input[id="password"]'
            ]

            # Try to find and fill username field
            for selector in username_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    await page.fill(selector, AUTH_CONFIG["username"])
                    print(f"Found username field with selector: {selector}")
                    break
                except:
                    continue

            # Try to find and fill password field
            for selector in password_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    await page.fill(selector, AUTH_CONFIG["password"])
                    print(f"Found password field with selector: {selector}")
                    break
                except:
                    continue

            # Try to find and click submit button
            submit_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("Sign in")',
                'button:has-text("Login")',
                'button:has-text("Submit")'
            ]

            for selector in submit_selectors:
                try:
                    await page.click(selector)
                    print(f"Clicked submit button with selector: {selector}")
                    break
                except:
                    continue

            # Wait for potential redirect or page change
            await page.wait_for_timeout(3000)

        except Exception as e:
            print(f"Authentication error: {e}")

    async def navigate_to_reports(self, page):
        """Navigate to the detailed report section directly."""
        print("Navigating to detailed reports section...")

        try:
            # Wait for the page to load
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(2000)

            # Method 1: Try direct navigation to agent_report
            print("Method 1: Attempting direct navigation to agent_report...")
            try:
                await page.goto("http://188.126.10.151:7080/public/agent_report/")
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(2000)

                current_url = page.url
                if "agent_report" in current_url:
                    print(f"[OK] Successfully navigated directly to detailed report: {current_url}")
                    return

            except Exception as e:
                print(f"[WARN] Direct navigation failed: {e}")

            # Method 2: Try clicking the link in the dropdown menu
            print("Method 2: Attempting to click the dropdown link...")
            try:
                # First, try to find and click the "Справки" dropdown
                reports_dropdown = page.locator('a.dropdown-toggle:has-text("Справки")')
                if await reports_dropdown.count() > 0:
                    await reports_dropdown.hover()
                    await page.wait_for_timeout(500)
                    print("[OK] Hovered over 'Справки' dropdown")

                    # Now find and click the detailed report link
                    detailed_link = page.locator('a:has-text("Детайлизирана справка")')
                    if await detailed_link.count() > 0:
                        await detailed_link.click()
                        await page.wait_for_load_state("domcontentloaded")
                        await page.wait_for_timeout(2000)

                        current_url = page.url
                        if "agent_report" in current_url:
                            print(f"[OK] Successfully navigated via dropdown to: {current_url}")
                            return
                        else:
                            print(f"[WARN] Unexpected URL after dropdown click: {current_url}")
                    else:
                        print("[WARN] Could not find 'Детайлизирана справка' link")
                else:
                    print("[WARN] Could not find 'Справки' dropdown")

            except Exception as e:
                print(f"[WARN] Dropdown navigation failed: {e}")

            # Method 3: Try finding the link by href attribute
            print("Method 3: Attempting to find link by href...")
            try:
                agent_report_link = page.locator('a[href*="agent_report"]')
                if await agent_report_link.count() > 0:
                    await agent_report_link.first.click()
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(2000)

                    current_url = page.url
                    if "agent_report" in current_url:
                        print(f"[OK] Successfully navigated via href to: {current_url}")
                        return
                    else:
                        print(f"[WARN] Unexpected URL after href click: {current_url}")
                else:
                    print("[WARN] Could not find link with href containing 'agent_report'")

            except Exception as e:
                print(f"[WARN] Href navigation failed: {e}")

            # If all methods fail
            print("[WARN] All navigation methods failed")

        except Exception as e:
            print(f"Error in navigate_to_reports: {e}")

    async def set_date_range(self, page):
        """Set the date range filter for a specific month."""
        if not self.target_year or not self.target_month:
            print("No target date specified, using default date range on page")
            return True

        try:
            print(f"Setting date range for {self.target_year}-{self.target_month:02d}...")

            # Calculate first and last day of the target month
            first_day = datetime(self.target_year, self.target_month, 1)
            # Get last day of month by going to first day of next month and subtracting one day
            if self.target_month == 12:
                last_day = datetime(self.target_year + 1, 1, 1) - pd.Timedelta(days=1)
            else:
                last_day = datetime(self.target_year, self.target_month + 1, 1) - pd.Timedelta(days=1)

            # Format dates as expected by the form (DD/MM/YYYY)
            date_from_str = first_day.strftime("%d/%m/%Y")
            date_to_str = last_day.strftime("%d/%m/%Y")

            print(f"Date range: {date_from_str} to {date_to_str}")

            # Wait for the date input fields to be available
            await page.wait_for_selector('#f_date_from', timeout=10000)
            await page.wait_for_selector('#f_date_to', timeout=10000)

            # Set dates using JavaScript to bypass datepicker
            await page.evaluate(f"""
                () => {{
                    document.getElementById('f_date_from').value = '{date_from_str}';
                    document.getElementById('f_date_to').value = '{date_to_str}';
                }}
            """)
            print(f"[OK] Set date_from to: {date_from_str}")
            print(f"[OK] Set date_to to: {date_to_str}")

            # Click the search button or call FindRows()
            try:
                # Try clicking the search button first
                search_button = page.locator('button.btn-info[onclick="FindRows()"]')
                if await search_button.count() > 0:
                    await search_button.click()
                    print("[OK] Clicked search button (FindRows)")
                else:
                    # Fallback: call FindRows() directly via JavaScript
                    await page.evaluate("FindRows()")
                    print("[OK] Called FindRows() via JavaScript")
            except Exception as e:
                print(f"[WARN] Could not click search button: {e}")
                # Last resort: try ChangePage(1)
                try:
                    await page.evaluate("ChangePage(1)")
                    print("[OK] Called ChangePage(1) as fallback")
                except Exception:
                    pass

            # Wait for AJAX request to complete and page to reload with new data
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(8000)  # Longer wait for AJAX data to load

            # Wait for table data to be reloaded - try multiple selectors
            try:
                # Try different selectors for the table rows
                selectors = [
                    'table.blueTable tbody tr.edit_rows',
                    'table.blueTable tbody tr',
                    '#page table tbody tr',
                    'table tbody tr'
                ]
                found = False
                for selector in selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=5000)
                        row_count = await page.locator(selector).count()
                        if row_count > 0:
                            print(f"[OK] Found {row_count} rows with selector: {selector}")
                            found = True
                            break
                    except Exception:
                        continue
                if not found:
                    print("[WARN] Could not find table rows after setting date range")
                await page.wait_for_timeout(2000)
            except Exception as e:
                print(f"[WARN] Error waiting for table: {e}")

            print(f"[OK] Date range set successfully for {self.target_year}-{self.target_month:02d}")
            return True

        except Exception as e:
            print(f"[WARN] Error setting date range: {e}")
            return False

    async def extract_data(self, page):
        """Extract data from the current page."""
        print("Extracting data from page...")

        try:
            # Wait for page to load completely
            await page.wait_for_load_state("networkidle")

            # Get page title and URL
            title = await page.title()
            url = page.url

            # Extract all text content
            text_content = await page.inner_text("body")

            # Check if we're on the detailed report page (agent_report)
            if "agent_report" in url or "Детайлизирана справка" in text_content:
                print("[OK] Detected detailed report page - extracting agent data...")
                # Extract data from all pages if pagination exists
                all_agent_data = await self.extract_all_agent_pages(page)

                # Create comprehensive page data
                page_data = {
                    "timestamp": datetime.now().isoformat(),
                    "url": url,
                    "title": title,
                    "page_type": "detailed_agent_report_complete",
                    "all_agents": all_agent_data["all_agents"],
                    "total_agents": all_agent_data["total_agents"],
                    "pages_processed": all_agent_data["pages_processed"],
                    "extraction_complete": all_agent_data["extraction_complete"]
                }

                if not all_agent_data["extraction_complete"] and "error" in all_agent_data:
                    page_data["error"] = all_agent_data["error"]

            else:
                print("[INFO] General data extraction...")
                page_data = await self.extract_general_data(page)

            self.scraped_data.append(page_data)
            print(f"Extracted data from: {title}")

            return page_data

        except Exception as e:
            print(f"Error extracting data: {e}")
            return None

    async def extract_agent_report_data(self, page):
        """Extract specific data from the detailed agent report page."""
        try:
            # Get basic page info
            title = await page.title()
            url = page.url

            # Wait for table to be fully loaded
            await page.wait_for_selector('table.blueTable', timeout=10000)

            # Wait for table rows to be present (try multiple selectors)
            selectors = [
                'table.blueTable tbody tr.edit_rows',
                'table.blueTable tbody tr[class*="edit"]',
                'table.blueTable tbody tr'
            ]
            for selector in selectors:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    row_count = await page.locator(selector).count()
                    if row_count > 0:
                        print(f"[INFO] Using row selector: {selector} ({row_count} rows)")
                        break
                except Exception:
                    continue

            # Additional wait to ensure JavaScript rendering is complete
            await page.wait_for_timeout(2000)

            # Extract the agent report table data with improved logic
            agent_data = await page.evaluate("""
                () => {
                    const table = document.querySelector('table.blueTable');
                    if (!table) {
                        console.log('No table with class blueTable found');
                        return null;
                    }

                    // Get the header row
                    const headerRow = table.querySelector('thead tr');
                    if (!headerRow) {
                        console.log('No header row found');
                        return null;
                    }

                    // Extract headers with better cleaning
                    const headers = Array.from(headerRow.querySelectorAll('th')).map(th => {
                        // Remove sorting spans and clean text
                        const span = th.querySelector('span');
                        if (span) {
                            span.remove();
                        }
                        return th.innerText.trim();
                    });

                    console.log('Headers found:', headers);

                    // Get data rows - specifically looking for edit_rows class
                    const dataRows = table.querySelectorAll('tbody tr.edit_rows');
                    console.log('Found', dataRows.length, 'agent rows');

                    // Extract data rows with better structure
                    const rows = Array.from(dataRows).map((row, rowIndex) => {
                        const cells = Array.from(row.querySelectorAll('td'));
                        const rowData = {
                            row_index: rowIndex,
                            agent_number: '',
                            agent_name: '',
                            incoming_calls: {},
                            outgoing_calls: {},
                            actions: ''
                        };

                        // Map cells to specific fields based on the HTML structure
                        cells.forEach((cell, cellIndex) => {
                            const cellText = cell.innerText.trim();
                            const headerName = headers[cellIndex] || `column_${cellIndex}`;

                            // Map to structured data based on column position
                            switch(cellIndex) {
                                case 0: // Agent Number
                                    rowData.agent_number = cellText;
                                    break;
                                case 1: // Agent Name
                                    rowData.agent_name = cellText;
                                    break;
                                case 2: // Incoming calls count
                                    rowData.incoming_calls.total = cellText;
                                    break;
                                case 3: // Incoming unsuccessful
                                    rowData.incoming_calls.unsuccessful = cellText;
                                    break;
                                case 4: // Incoming successful
                                    rowData.incoming_calls.successful = cellText;
                                    break;
                                case 5: // Incoming min time
                                    rowData.incoming_calls.min_time = cellText;
                                    break;
                                case 6: // Incoming max time
                                    rowData.incoming_calls.max_time = cellText;
                                    break;
                                case 7: // Incoming average time
                                    rowData.incoming_calls.avg_time = cellText;
                                    break;
                                case 8: // Incoming total time
                                    rowData.incoming_calls.total_time = cellText;
                                    break;
                                case 9: // Incoming min wait
                                    rowData.incoming_calls.min_wait = cellText;
                                    break;
                                case 10: // Incoming max wait
                                    rowData.incoming_calls.max_wait = cellText;
                                    break;
                                case 11: // Incoming average wait
                                    rowData.incoming_calls.avg_wait = cellText;
                                    break;
                                case 12: // Outgoing calls count
                                    rowData.outgoing_calls.total = cellText;
                                    break;
                                case 13: // Outgoing unsuccessful
                                    rowData.outgoing_calls.unsuccessful = cellText;
                                    break;
                                case 14: // Outgoing successful
                                    rowData.outgoing_calls.successful = cellText;
                                    break;
                                case 15: // Outgoing min time
                                    rowData.outgoing_calls.min_time = cellText;
                                    break;
                                case 16: // Outgoing max time
                                    rowData.outgoing_calls.max_time = cellText;
                                    break;
                                case 17: // Outgoing average time
                                    rowData.outgoing_calls.avg_time = cellText;
                                    break;
                                case 18: // Outgoing total time
                                    rowData.outgoing_calls.total_time = cellText;
                                    break;
                                case 19: // Actions column
                                    rowData.actions = cellText;
                                    break;
                            }

                            // Also store raw data with header names
                            rowData[`raw_${headerName}`] = cellText;
                        });

                        return rowData;
                    });

                    // Get pagination info if available
                    const paginationInfo = {};
                    const paginationFooter = table.querySelector('tfoot');
                    if (paginationFooter) {
                        const links = Array.from(paginationFooter.querySelectorAll('a'));
                        paginationInfo.total_pages = links.length;
                        const activeLink = paginationFooter.querySelector('a.active');
                        if (activeLink) {
                            paginationInfo.current_page = activeLink.innerText.trim();
                        }
                    }

                    return {
                        headers: headers,
                        rows: rows,
                        total_rows: rows.length,
                        pagination: paginationInfo,
                        table_found: true
                    };
                }
            """)

            # Extract filter information with better error handling
            filters = await page.evaluate("""
                () => {
                    const filters = {};

                    try {
                        // Get current filter values
                        const cidInput = document.querySelector('#f_cid');
                        const dateFromInput = document.querySelector('#f_date_from');
                        const dateToInput = document.querySelector('#f_date_to');
                        const lineInput = document.querySelector('#f_line');
                        const trunkSelect = document.querySelector('#f_trunk');

                        if (cidInput) filters.phone_number = cidInput.value || '';
                        if (dateFromInput) filters.date_from = dateFromInput.value || '';
                        if (dateToInput) filters.date_to = dateToInput.value || '';
                        if (lineInput) filters.line = lineInput.value || '';
                        if (trunkSelect) {
                            filters.operator_value = trunkSelect.value || '';
                            filters.operator_text = trunkSelect.options[trunkSelect.selectedIndex]?.text || '';
                        }
                    } catch (e) {
                        console.log('Error extracting filters:', e);
                    }

                    return filters;
                }
            """)

            # Log extraction results
            if agent_data and agent_data['table_found']:
                print(f"[OK] Successfully extracted data for {agent_data['total_rows']} agents")
                for i, row in enumerate(agent_data['rows'][:3]):  # Show first 3 agents
                    print(f"  Agent {i+1}: {row['agent_number']} - {row['agent_name']}")
                if agent_data['total_rows'] > 3:
                    print(f"  ... and {agent_data['total_rows'] - 3} more agents")
            else:
                print("[WARN] No agent data extracted or table not found")

            return {
                "timestamp": datetime.now().isoformat(),
                "url": url,
                "title": title,
                "page_type": "detailed_agent_report",
                "filters": filters,
                "agent_data": agent_data,
                "total_agents": agent_data["total_rows"] if agent_data else 0,
                "extraction_successful": agent_data is not None and agent_data.get('table_found', False)
            }

        except Exception as e:
            print(f"Error extracting agent report data: {e}")
            return await self.extract_general_data(page)

    async def extract_general_data(self, page):
        """Extract general data from any page."""
        try:
            title = await page.title()
            url = page.url
            text_content = await page.inner_text("body")

            # Extract all links
            links = await page.evaluate("""
                () => {
                    const links = Array.from(document.querySelectorAll('a[href]'));
                    return links.map(link => ({
                        text: link.innerText.trim(),
                        href: link.href
                    }));
                }
            """)

            # Extract tables if any
            tables = await page.evaluate("""
                () => {
                    const tables = Array.from(document.querySelectorAll('table'));
                    return tables.map((table, index) => {
                        const rows = Array.from(table.querySelectorAll('tr'));
                        return {
                            table_index: index,
                            rows: rows.map(row => {
                                const cells = Array.from(row.querySelectorAll('td, th'));
                                return cells.map(cell => cell.innerText.trim());
                            })
                        };
                    });
                }
            """)

            return {
                "timestamp": datetime.now().isoformat(),
                "url": url,
                "title": title,
                "page_type": "general",
                "text_content": text_content[:1000] + "..." if len(text_content) > 1000 else text_content,
                "links": links,
                "tables": tables
            }

        except Exception as e:
            print(f"Error in general data extraction: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "url": page.url,
                "title": await page.title(),
                "page_type": "error",
                "error": str(e)
            }

    async def extract_all_agent_pages(self, page):
        """Extract data from all pages of the agent report table."""
        print("Extracting data from all agent report pages...")
        all_agents = []
        current_page = 1
        total_pages = 1

        try:
            while current_page <= total_pages:
                print(f"Extracting page {current_page}...")

                # Extract data from current page
                page_data = await self.extract_agent_report_data(page)

                if page_data and page_data.get('extraction_successful'):
                    agents_on_page = page_data['agent_data']['rows']
                    all_agents.extend(agents_on_page)

                    # Update total pages from pagination info
                    pagination = page_data['agent_data'].get('pagination', {})
                    if pagination.get('total_pages'):
                        total_pages = pagination['total_pages']

                    print(f"[OK] Extracted {len(agents_on_page)} agents from page {current_page}")

                    # Check if there are more pages
                    if current_page < total_pages:
                        # Try to navigate to next page
                        next_page_success = await self.navigate_to_next_page(page, current_page + 1)
                        if not next_page_success:
                            print(f"Could not navigate to page {current_page + 1}")
                            break

                    current_page += 1
                else:
                    print(f"Failed to extract data from page {current_page}")
                    break

                # Add delay between pages
                await page.wait_for_timeout(1000)

            print(f"[OK] Total agents extracted: {len(all_agents)} from {current_page - 1} pages")

            return {
                "all_agents": all_agents,
                "total_agents": len(all_agents),
                "pages_processed": current_page - 1,
                "extraction_complete": True
            }

        except Exception as e:
            print(f"Error extracting all agent pages: {e}")
            return {
                "all_agents": all_agents,
                "total_agents": len(all_agents),
                "pages_processed": current_page - 1,
                "extraction_complete": False,
                "error": str(e)
            }

    async def navigate_to_next_page(self, page, page_number):
        """Navigate to a specific page in the agent table pagination."""
        try:
            # Method 1: Use JavaScript ChangePage function directly (most reliable)
            await page.evaluate(f'ChangePage({page_number})')
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)
            return True

        except Exception as e:
            try:
                # Method 2: Try clicking the page link in the pagination area (tfoot)
                page_link = page.locator(f'tfoot a[onclick*="ChangePage(\'{page_number}\')"]')
                if await page_link.count() > 0:
                    await page_link.click()
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(2000)
                    return True
            except Exception:
                pass

            print(f"Error navigating to page {page_number}: {e}")
            return False

    async def scrape(self):
        """Main scraping function."""
        print(f"Starting scraper for: {BASE_URL}")

        async with async_playwright() as playwright:
            browser, page = await self.setup_browser(playwright)

            try:
                # Navigate to the target URL
                print(f"Navigating to: {BASE_URL}")
                await page.goto(BASE_URL)

                # Handle authentication if needed
                await self.handle_authentication(page)

                # Wait for the page to load after potential login
                await page.wait_for_timeout(SCRAPING_CONFIG["wait_for_load"] * 1000)

                # Navigate to the specific report section
                await self.navigate_to_reports(page)

                # Set the date range if target month is specified
                await self.set_date_range(page)

                # Extract data from the main page
                await self.extract_data(page)

            except Exception as e:
                print(f"Scraping error: {e}")

            finally:
                await browser.close()

        # Scraping completed - data is stored in self.scraped_data
        print(f"Scraping completed. {len(self.scraped_data)} records extracted.")

        # Removed save_data method - no longer saving files automatically
    # Data is now stored only in MongoDB via the Flask app
    # File downloads are available on-demand through API endpoints

    async def extract_agent_call_details(self, page, agent_number, agent_name):
        """Extract individual call details from agent modal."""
        try:
            print(f"Extracting call details for agent {agent_number} ({agent_name})...")
            
            # Find and click the agent row to open the modal
            agent_row_selector = f'tr.edit_rows:has-text("{agent_number}")'
            await page.click(agent_row_selector)
            
            # Wait for modal to appear
            await page.wait_for_selector('.modal-body', timeout=10000)
            await page.wait_for_timeout(2000)  # Allow modal content to load
            
            # Extract incoming calls data
            incoming_calls = await page.evaluate("""
                () => {
                    const modalBody = document.querySelector('.modal-body');
                    if (!modalBody) return [];
                    
                    // Find the incoming calls table (first table after "Входящи" heading)
                    const incomingHeading = Array.from(modalBody.querySelectorAll('h3')).find(h => h.textContent.includes('Входящи'));
                    if (!incomingHeading) return [];
                    
                    const incomingTable = incomingHeading.nextElementSibling;
                    if (!incomingTable || incomingTable.tagName !== 'TABLE') return [];
                    
                    const rows = Array.from(incomingTable.querySelectorAll('tbody tr.edit_rows1'));
                    
                    return rows.map(row => {
                        const cells = Array.from(row.querySelectorAll('td'));
                        return {
                            call_number: cells[0]?.textContent?.trim() || '',
                            date_time: cells[1]?.textContent?.trim() || '',
                            initiator: cells[2]?.textContent?.trim() || '',
                            recipient: cells[3]?.textContent?.trim() || '',
                            total_duration: cells[4]?.textContent?.trim() || '',
                            wait_time: cells[5]?.textContent?.trim() || '',
                            talk_time: cells[6]?.textContent?.trim() || '',
                            status: cells[7]?.textContent?.trim() || ''
                        };
                    });
                }
            """)
            
            # Extract outgoing calls data
            outgoing_calls = await page.evaluate("""
                () => {
                    const modalBody = document.querySelector('.modal-body');
                    if (!modalBody) return [];
                    
                    // Find the outgoing calls table (table after "Изходящи" heading)
                    const outgoingHeading = Array.from(modalBody.querySelectorAll('h3')).find(h => h.textContent.includes('Изходящи'));
                    if (!outgoingHeading) return [];
                    
                    const outgoingTable = outgoingHeading.nextElementSibling;
                    if (!outgoingTable || outgoingTable.tagName !== 'TABLE') return [];
                    
                    const rows = Array.from(outgoingTable.querySelectorAll('tbody tr.edit_rows1'));
                    
                    return rows.map(row => {
                        const cells = Array.from(row.querySelectorAll('td'));
                        return {
                            call_number: cells[0]?.textContent?.trim() || '',
                            date_time: cells[1]?.textContent?.trim() || '',
                            initiator: cells[2]?.textContent?.trim() || '',
                            recipient: cells[3]?.textContent?.trim() || '',
                            total_duration: cells[4]?.textContent?.trim() || '',
                            talk_time: cells[5]?.textContent?.trim() || '',
                            status: cells[6]?.textContent?.trim() || ''
                        };
                    });
                }
            """)
            
            # Close the modal by clicking outside or on close button
            try:
                await page.click('.modal-backdrop', timeout=2000)
            except:
                try:
                    await page.click('.modal .close', timeout=2000)
                except:
                    # Press Escape key as fallback
                    await page.keyboard.press('Escape')
            
            await page.wait_for_timeout(1000)  # Wait for modal to close
            
            print(f"[OK] Extracted {len(incoming_calls)} incoming and {len(outgoing_calls)} outgoing calls for agent {agent_number}")
            
            return {
                "agent_number": agent_number,
                "agent_name": agent_name,
                "incoming_calls": incoming_calls,
                "outgoing_calls": outgoing_calls,
                "total_calls": len(incoming_calls) + len(outgoing_calls)
            }
            
        except Exception as e:
            print(f"[WARN] Error extracting call details for agent {agent_number}: {e}")
            return {
                "agent_number": agent_number,
                "agent_name": agent_name,
                "incoming_calls": [],
                "outgoing_calls": [],
                "total_calls": 0,
                "error": str(e)
            }

    async def scrape_with_call_details(self, max_agents=None):
        """Enhanced scraping that includes individual agent call details."""
        async with async_playwright() as playwright:
            browser, page = await self.setup_browser(playwright)

            try:
                print("Starting enhanced scraping with call details...")
                
                # Navigate to the base URL
                print(f"Navigating to: {BASE_URL}")
                await page.goto(BASE_URL, wait_until="domcontentloaded")

                # Handle authentication if needed
                await self.handle_authentication(page)

                # Navigate to reports section
                await self.navigate_to_reports(page)

                # Extract basic agent data first
                basic_data = await self.extract_agent_report_data(page)
                
                if not basic_data or not basic_data.get('extraction_successful'):
                    print("[WARN] Failed to extract basic agent data")
                    return self.scraped_data

                agents = basic_data.get('agent_data', {}).get('rows', [])
                
                if not agents:
                    print("[WARN] No agents found")
                    return self.scraped_data

                # Limit agents if specified
                if max_agents and max_agents > 0:
                    agents = agents[:max_agents]
                    print(f"Limiting to first {max_agents} agents")

                enhanced_agents = []
                
                for i, agent in enumerate(agents):
                    try:
                        agent_number = agent.get('agent_number', '')
                        agent_name = agent.get('agent_name', '')
                        
                        print(f"Processing agent {i+1}/{len(agents)}: {agent_number} - {agent_name}")
                        
                        # Get call details for this agent
                        call_details = await self.extract_agent_call_details(page, agent_number, agent_name)
                        
                        # Combine all data
                        enhanced_agent = {
                            **agent,  # Original agent data
                            'call_details': call_details
                        }
                        
                        enhanced_agents.append(enhanced_agent)
                        
                        # Add a small delay between agents to avoid overwhelming the server
                        await page.wait_for_timeout(1000)
                        
                    except Exception as e:
                        print(f"[WARN] Error processing agent {agent.get('agent_number', 'unknown')}: {e}")
                        # Add agent without call details
                        enhanced_agents.append({
                            **agent,
                            'call_details': {
                                'agent_number': agent.get('agent_number', ''),
                                'agent_name': agent.get('agent_name', ''),
                                'incoming_calls': [],
                                'outgoing_calls': [],
                                'total_calls': 0,
                                'error': str(e)
                            }
                        })

                # Store enhanced data
                enhanced_data = {
                    **basic_data,
                    'agent_data': {
                        **basic_data.get('agent_data', {}),
                        'rows': enhanced_agents
                    },
                    'enhancement_info': {
                        'call_details_extracted': True,
                        'processed_agents': len(enhanced_agents),
                        'max_agents_limit': max_agents
                    }
                }
                
                self.scraped_data = [enhanced_data]
                
                print(f"[OK] Enhanced scraping completed for {len(enhanced_agents)} agents")
                
                return self.scraped_data

            except Exception as e:
                print(f"Error in enhanced scraping: {e}")
                return self.scraped_data

            finally:
                await browser.close()


async def main():
    """Main entry point."""
    scraper = AgentReportScraper()
    await scraper.scrape()


if __name__ == "__main__":
    asyncio.run(main())