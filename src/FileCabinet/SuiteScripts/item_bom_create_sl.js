/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 * @NModuleScope SameAccount
 */

/**
 * BOM Import Suitelet
 * 
 * Purpose: Provide UI for uploading CSV files, mapping columns, and triggering BOM import
 * 
 * Flow:
 * 1. User uploads CSV file and enters prospect name
 * 2. Suitelet shows preview (headers + first 10 rows) with column mapping dropdowns
 * 3. User maps columns and clicks Import
 * 4. Suitelet saves config and triggers Map/Reduce script
 * 5. User sees confirmation with link to monitor progress
 */

define(['N/ui/serverWidget', 'N/file', 'N/task', 'N/runtime', 'N/redirect', 'N/url', 'N/log'],
    function(serverWidget, file, task, runtime, redirect, url, log) {

        // Field mapping options - what columns can be mapped to
        const FIELD_OPTIONS = [
            { value: '', text: '-- Skip --' },
            { value: 'hierarchy', text: 'Hierarchy (Required)' },
            { value: 'itemid', text: 'Item ID / Part Number (Required)' },
            { value: 'displayname', text: 'Display Name / Description (sets all 3)' },
            { value: 'mpn', text: 'Manufacturer Part Number' },
            { value: 'manufacturer', text: 'Manufacturer' },
            { value: 'vendor', text: 'Vendor Name (for Create Vendors option)' },
            { value: 'vendorpartnumber', text: 'Vendor Part Number' },
            { value: 'quantity', text: 'BOM Quantity' },
            { value: 'revision', text: 'Item Revision' },
            { value: 'custitem_pkg_info', text: 'Package Info (Custom)' },
            { value: 'custitem_part_requirement', text: 'Part Requirement (Custom)' },
            { value: 'memo', text: 'Comments / Memo' }
        ];

        /**
         * Main entry point
         */
        function onRequest(context) {
            try {
                if (context.request.method === 'GET') {
                    showUploadForm(context);
                } else {
                    handlePost(context);
                }
            } catch (e) {
                log.error('Suitelet Error', e.toString() + '\n' + e.stack);
                showError(context, e.toString());
            }
        }

        /**
         * Step 1: Show file upload form
         */
        function showUploadForm(context) {
            const form = serverWidget.createForm({
                title: 'BOM Import - Step 1: Upload File'
            });

            // Instructions
            const instructionsHtml = `
                <div style="margin-bottom: 20px; padding: 15px; background-color: #f5f5f5; border-radius: 5px;">
                    <h3 style="margin-top: 0;">Instructions</h3>
                    <ol>
                        <li>Enter a <strong>Prospect Name</strong> - this will be used as a prefix for all External IDs 
                            (e.g., "AcmeCorp" → "AcmeCorp_PartNumber123")</li>
                        <li>Upload your <strong>CSV file</strong> containing the BOM data</li>
                        <li>Your CSV should have a <strong>Hierarchy column</strong> with values like 1.0, 1.1, 1.1.1 to define structure</li>
                        <li>Click Next to preview and map columns</li>
                    </ol>
                </div>
            `;
            
            const instructionsField = form.addField({
                id: 'custpage_instructions',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Instructions'
            });
            instructionsField.defaultValue = instructionsHtml;

            // Prospect Name
            const prospectField = form.addField({
                id: 'custpage_prospect_name',
                type: serverWidget.FieldType.TEXT,
                label: 'Prospect Name'
            });
            prospectField.isMandatory = true;
            prospectField.setHelpText({
                help: 'Used as prefix for External IDs to prevent collisions between prospects. Example: "NASElectronics" or "AcmeCorp"'
            });

            // File Upload
            const fileField = form.addField({
                id: 'custpage_csv_file',
                type: serverWidget.FieldType.FILE,
                label: 'CSV File'
            });
            fileField.isMandatory = true;

            // Hidden field to track step
            const stepField = form.addField({
                id: 'custpage_step',
                type: serverWidget.FieldType.TEXT,
                label: 'Step'
            });
            stepField.defaultValue = 'upload';
            stepField.updateDisplayType({ displayType: serverWidget.FieldDisplayType.HIDDEN });

            form.addSubmitButton({ label: 'Next: Preview & Map Columns' });

            context.response.writePage(form);
        }

        /**
         * Handle POST requests (multiple steps)
         */
        function handlePost(context) {
            const step = context.request.parameters.custpage_step;

            if (step === 'upload') {
                handleFileUpload(context);
            } else if (step === 'mapping') {
                handleMappingSubmit(context);
            }
        }

        /**
         * Step 2: Process uploaded file and show mapping form
         */
        function handleFileUpload(context) {
            const prospectName = context.request.parameters.custpage_prospect_name;
            const uploadedFile = context.request.files.custpage_csv_file;

            if (!uploadedFile) {
                throw new Error('No file uploaded');
            }

            // Get folder ID from script parameter
            const scriptObj = runtime.getCurrentScript();
            const folderId = scriptObj.getParameter({ name: 'custscript_bom_suitelet_folder_id' });

            if (!folderId) {
                throw new Error('Folder ID parameter not configured. Please set custscript_bom_suitelet_folder_id on the script deployment.');
            }

            // Save file to folder
            uploadedFile.folder = folderId;
            const fileId = uploadedFile.save();

            log.audit('File Uploaded', 'File ID: ' + fileId + ', Name: ' + uploadedFile.name);

            // Load and parse file
            const fileObj = file.load({ id: fileId });
            const content = fileObj.getContents();
            const parsedData = parseCSV(content);

            if (parsedData.rows.length === 0) {
                throw new Error('No data rows found in file');
            }

            // Show mapping form
            showMappingForm(context, prospectName, fileId, uploadedFile.name, parsedData);
        }

        /**
         * Show column mapping form with preview
         */
        function showMappingForm(context, prospectName, fileId, fileName, parsedData) {
            const form = serverWidget.createForm({
                title: 'BOM Import - Step 2: Map Columns'
            });

            // Info section
            const infoHtml = `
                <div style="margin-bottom: 20px; padding: 15px; background-color: #e8f4e8; border-radius: 5px; border-left: 4px solid #28a745;">
                    <strong>Prospect:</strong> ${escapeHtml(prospectName)}<br>
                    <strong>File:</strong> ${escapeHtml(fileName)}<br>
                    <strong>Rows Found:</strong> ${parsedData.rows.length} data rows
                </div>
                <div style="margin-bottom: 15px; padding: 10px; background-color: #fff3cd; border-radius: 5px; border-left: 4px solid #ffc107;">
                    <strong>Required Mappings:</strong> You must map at least <strong>Hierarchy</strong> and <strong>Item ID / Part Number</strong>
                </div>
            `;

            form.addField({
                id: 'custpage_info',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Info'
            }).defaultValue = infoHtml;

            // Hidden fields
            form.addField({
                id: 'custpage_step',
                type: serverWidget.FieldType.TEXT,
                label: 'Step'
            }).defaultValue = 'mapping';
            form.getField({ id: 'custpage_step' }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.HIDDEN });

            form.addField({
                id: 'custpage_prospect_name',
                type: serverWidget.FieldType.TEXT,
                label: 'Prospect Name'
            }).defaultValue = prospectName;
            form.getField({ id: 'custpage_prospect_name' }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.HIDDEN });

            form.addField({
                id: 'custpage_file_id',
                type: serverWidget.FieldType.TEXT,
                label: 'File ID'
            }).defaultValue = fileId.toString();
            form.getField({ id: 'custpage_file_id' }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.HIDDEN });

            form.addField({
                id: 'custpage_file_name',
                type: serverWidget.FieldType.TEXT,
                label: 'File Name'
            }).defaultValue = fileName;
            form.getField({ id: 'custpage_file_name' }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.HIDDEN });

            form.addField({
                id: 'custpage_column_count',
                type: serverWidget.FieldType.INTEGER,
                label: 'Column Count'
            }).defaultValue = parsedData.headers.length;
            form.getField({ id: 'custpage_column_count' }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.HIDDEN });

            // Defaults Section
            form.addFieldGroup({
                id: 'custpage_defaults_group',
                label: 'Item Defaults'
            });

            // Setup MRP Checkbox
            const mrpCheckbox = form.addField({
                id: 'custpage_setup_mrp',
                type: serverWidget.FieldType.CHECKBOX,
                label: 'Setup MRP',
                container: 'custpage_defaults_group'
            });
            mrpCheckbox.defaultValue = 'T';
            mrpCheckbox.setHelpText({ help: 'When checked: Assembly items use Master Production Scheduling, Inventory items use Material Requirements Planning. Creates Planning Item Category using Prospect Name. Configures varied lead times and lot sizing for demo.' });

            // Create Vendors Checkbox
            const createVendorsCheckbox = form.addField({
                id: 'custpage_create_vendors',
                type: serverWidget.FieldType.CHECKBOX,
                label: 'Create Vendors from CSV',
                container: 'custpage_defaults_group'
            });
            createVendorsCheckbox.defaultValue = 'F';
            createVendorsCheckbox.setHelpText({ help: 'When checked: Creates vendor records from the Vendor column in CSV and links them to items. When unchecked: Uses the Default Vendor ID below.' });

            // Default Vendor (used when Create Vendors is unchecked)
            const vendorField = form.addField({
                id: 'custpage_vendor_id',
                type: serverWidget.FieldType.INTEGER,
                label: 'Default Vendor ID',
                container: 'custpage_defaults_group'
            });
            vendorField.defaultValue = 625;
            vendorField.setHelpText({ help: 'Internal ID of vendor to add to inventory items when "Create Vendors from CSV" is unchecked (0 = none)' });

            // Purchase Price
            const purchasePriceField = form.addField({
                id: 'custpage_purchase_price',
                type: serverWidget.FieldType.CURRENCY,
                label: 'Default Purchase Price',
                container: 'custpage_defaults_group'
            });
            purchasePriceField.defaultValue = 1;

            // Tax Schedule
            const taxScheduleField = form.addField({
                id: 'custpage_tax_schedule',
                type: serverWidget.FieldType.INTEGER,
                label: 'Tax Schedule ID',
                container: 'custpage_defaults_group'
            });
            taxScheduleField.defaultValue = 1;
            taxScheduleField.setHelpText({ help: 'Internal ID of the tax schedule to apply to items' });

            // Locations
            const locationsField = form.addField({
                id: 'custpage_locations',
                type: serverWidget.FieldType.TEXT,
                label: 'Location IDs (comma-separated)',
                container: 'custpage_defaults_group'
            });
            locationsField.defaultValue = '2,13';
            locationsField.setHelpText({ help: 'Internal IDs of locations to configure for each item' });

            // Item Location Defaults Group (shown when MRP is unchecked)
            form.addFieldGroup({
                id: 'custpage_loc_defaults_group',
                label: 'Item Location Defaults (used when Setup MRP is unchecked)'
            });

            const prefStockField = form.addField({
                id: 'custpage_pref_stock',
                type: serverWidget.FieldType.INTEGER,
                label: 'Preferred Stock Level',
                container: 'custpage_loc_defaults_group'
            });
            prefStockField.defaultValue = 1000;

            const reorderPointField = form.addField({
                id: 'custpage_reorder_point',
                type: serverWidget.FieldType.INTEGER,
                label: 'Reorder Point',
                container: 'custpage_loc_defaults_group'
            });
            reorderPointField.defaultValue = 600;

            const safetyStockField = form.addField({
                id: 'custpage_safety_stock',
                type: serverWidget.FieldType.INTEGER,
                label: 'Safety Stock Level',
                container: 'custpage_loc_defaults_group'
            });
            safetyStockField.defaultValue = 100;

            const leadTimeField = form.addField({
                id: 'custpage_lead_time',
                type: serverWidget.FieldType.INTEGER,
                label: 'Lead Time (days)',
                container: 'custpage_loc_defaults_group'
            });
            leadTimeField.defaultValue = 7;

            // Column Mapping Section
            form.addFieldGroup({
                id: 'custpage_mapping_group',
                label: 'Column Mapping'
            });

            // Create mapping dropdown for each column
            parsedData.headers.forEach((header, index) => {
                const selectField = form.addField({
                    id: 'custpage_map_col_' + index,
                    type: serverWidget.FieldType.SELECT,
                    label: header || ('Column ' + (index + 1)),
                    container: 'custpage_mapping_group'
                });

                // Add options
                FIELD_OPTIONS.forEach(opt => {
                    selectField.addSelectOption({
                        value: opt.value,
                        text: opt.text
                    });
                });

                // Auto-select based on header name
                const autoMap = autoDetectMapping(header);
                if (autoMap) {
                    selectField.defaultValue = autoMap;
                }
            });

            // Preview Section
            form.addFieldGroup({
                id: 'custpage_preview_group',
                label: 'Data Preview (First 10 Rows)'
            });

            // Build preview table
            let previewHtml = '<div style="overflow-x: auto;"><table style="border-collapse: collapse; width: 100%; font-size: 12px;">';
            
            // Header row
            previewHtml += '<tr style="background-color: #4a5568; color: white;">';
            parsedData.headers.forEach(header => {
                previewHtml += '<th style="padding: 8px; border: 1px solid #ddd; text-align: left;">' + escapeHtml(header || '(empty)') + '</th>';
            });
            previewHtml += '</tr>';

            // Data rows (first 10)
            const previewRows = parsedData.rows.slice(0, 10);
            previewRows.forEach((row, rowIndex) => {
                const bgColor = rowIndex % 2 === 0 ? '#ffffff' : '#f8f9fa';
                previewHtml += '<tr style="background-color: ' + bgColor + ';">';
                parsedData.headers.forEach((_, colIndex) => {
                    const cellValue = row[colIndex] || '';
                    previewHtml += '<td style="padding: 6px; border: 1px solid #ddd;">' + escapeHtml(cellValue) + '</td>';
                });
                previewHtml += '</tr>';
            });

            previewHtml += '</table></div>';

            if (parsedData.rows.length > 10) {
                previewHtml += '<p style="color: #666; font-style: italic;">... and ' + (parsedData.rows.length - 10) + ' more rows</p>';
            }

            form.addField({
                id: 'custpage_preview',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Preview',
                container: 'custpage_preview_group'
            }).defaultValue = previewHtml;

            form.addSubmitButton({ label: 'Run Import' });
            form.addButton({
                id: 'custpage_back',
                label: 'Back',
                functionName: 'history.back()'
            });

            context.response.writePage(form);
        }

        /**
         * Auto-detect field mapping based on header name
         */
        function autoDetectMapping(header) {
            if (!header) return null;
            
            const h = header.toLowerCase().trim();

            if (h.includes('hierarchy') || h.includes('bom structure') || h.includes('level')) {
                return 'hierarchy';
            }
            if (h.includes('part number') || h.includes('part #') || h.includes('itemid') || h === 'part' || h === 'item') {
                return 'itemid';
            }
            if (h.includes('description') || h.includes('name')) {
                return 'displayname';
            }
            if (h.includes('qty') || h.includes('quantity') || h.includes('count')) {
                return 'quantity';
            }
            if (h.includes('mpn') || h.includes('mfg part') || h.includes('manu') || h.includes('manufacturer part')) {
                return 'mpn';
            }
            if (h.includes('vendor') && h.includes('number')) {
                return 'vendorpartnumber';
            }
            if (h.includes('vendor')) {
                return 'vendor';
            }
            if (h.includes('revision') || h === 'rev') {
                return 'revision';
            }
            if (h.includes('comment') || h.includes('memo') || h.includes('note')) {
                return 'memo';
            }

            return null;
        }

        /**
         * Step 3: Process mapping and trigger import
         */
        function handleMappingSubmit(context) {
            const params = context.request.parameters;
            const prospectName = params.custpage_prospect_name;
            const fileId = params.custpage_file_id;
            const fileName = params.custpage_file_name;
            const columnCount = parseInt(params.custpage_column_count);

            // Collect mappings
            const mappings = {};
            let hasHierarchy = false;
            let hasItemId = false;

            for (let i = 0; i < columnCount; i++) {
                const mapValue = params['custpage_map_col_' + i];
                if (mapValue) {
                    mappings[i] = mapValue;
                    if (mapValue === 'hierarchy') hasHierarchy = true;
                    if (mapValue === 'itemid') hasItemId = true;
                }
            }

            // Validate required mappings
            if (!hasHierarchy) {
                throw new Error('Hierarchy column must be mapped');
            }
            if (!hasItemId) {
                throw new Error('Item ID / Part Number column must be mapped');
            }

            // Collect defaults
            const locationStr = params.custpage_locations || '2,13';
            const locationIds = locationStr.split(',').map(id => parseInt(id.trim())).filter(id => !isNaN(id) && id > 0);

            const setupMRP = params.custpage_setup_mrp === 'T';
            const createVendors = params.custpage_create_vendors === 'T';

            const defaults = {
                setupMRP: setupMRP,
                createVendors: createVendors,
                taxScheduleId: parseInt(params.custpage_tax_schedule) || 1,
                vendorId: parseInt(params.custpage_vendor_id) || 0,
                purchasePrice: parseFloat(params.custpage_purchase_price) || 1,
                locationIds: locationIds,
                itemLocationDefaults: {
                    preferredstocklevel: parseInt(params.custpage_pref_stock) || 1000,
                    reorderpoint: parseInt(params.custpage_reorder_point) || 600,
                    safetystocklevel: parseInt(params.custpage_safety_stock) || 100,
                    leadtime: parseInt(params.custpage_lead_time) || 7
                }
            };

            log.audit('Mappings Collected', JSON.stringify(mappings));
            log.audit('Defaults Collected', JSON.stringify(defaults));

            // Save config JSON file alongside CSV
            const configData = {
                prospectName: prospectName,
                csvFileId: fileId,
                csvFileName: fileName,
                mappings: mappings,
                defaults: defaults,
                createdDate: new Date().toISOString()
            };

            const scriptObj = runtime.getCurrentScript();
            const folderId = scriptObj.getParameter({ name: 'custscript_bom_suitelet_folder_id' });

            const configFileName = fileName.replace(/\.csv$/i, '') + '_config.json';
            const configFile = file.create({
                name: configFileName,
                fileType: file.Type.JSON,
                contents: JSON.stringify(configData, null, 2),
                folder: folderId
            });
            const configFileId = configFile.save();

            log.audit('Config Saved', 'Config File ID: ' + configFileId);

            // Trigger Map/Reduce script
            const mrScriptId = scriptObj.getParameter({ name: 'custscript_bom_mr_script_id' });
            const mrDeploymentId = scriptObj.getParameter({ name: 'custscript_bom_mr_deployment_id' });

            if (!mrScriptId || !mrDeploymentId) {
                throw new Error('Map/Reduce script ID and deployment ID must be configured in script parameters');
            }

            const mrTask = task.create({
                taskType: task.TaskType.MAP_REDUCE,
                scriptId: mrScriptId,
                deploymentId: mrDeploymentId,
                params: {
                    'custscript_bom_config_file_id': configFileId
                }
            });

            const taskId = mrTask.submit();

            log.audit('Map/Reduce Triggered', 'Task ID: ' + taskId);

            // Show confirmation page
            showConfirmation(context, prospectName, fileName, taskId, configFileId);
        }

        /**
         * Show confirmation page with task monitoring link
         */
        function showConfirmation(context, prospectName, fileName, taskId, configFileId) {
            const form = serverWidget.createForm({
                title: 'BOM Import - Started'
            });

            // Build monitoring URL
            const taskStatusUrl = url.resolveScript({
                scriptId: runtime.getCurrentScript().id,
                deploymentId: runtime.getCurrentScript().deploymentId,
                params: { checkTask: taskId }
            });

            const confirmHtml = `
                <div style="padding: 20px; background-color: #d4edda; border-radius: 5px; border-left: 4px solid #28a745; margin-bottom: 20px;">
                    <h2 style="margin-top: 0; color: #155724;">✓ Import Started Successfully</h2>
                    <p><strong>Prospect:</strong> ${escapeHtml(prospectName)}</p>
                    <p><strong>File:</strong> ${escapeHtml(fileName)}</p>
                    <p><strong>Task ID:</strong> ${escapeHtml(taskId)}</p>
                </div>

                <div style="padding: 15px; background-color: #f8f9fa; border-radius: 5px; margin-bottom: 20px;">
                    <h3>What Happens Next?</h3>
                    <ol>
                        <li>The Map/Reduce script will process your file</li>
                        <li>Items will be created first (inventory items, then assembly items)</li>
                        <li>BOMs and BOM Revisions will be created for assemblies</li>
                        <li>Check the Execution Log for progress and any errors</li>
                    </ol>
                </div>

                <div style="padding: 15px; background-color: #e7f3ff; border-radius: 5px;">
                    <h3>Monitor Progress</h3>
                    <p>View the script execution log to monitor progress:</p>
                    <p>
                        <strong>Customization → Scripting → Script Deployments</strong> → 
                        Find BOM Import deployment → View Execution Log
                    </p>
                    <p>Or search for Task ID: <code>${escapeHtml(taskId)}</code> in the logs</p>
                </div>
            `;

            form.addField({
                id: 'custpage_confirmation',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Confirmation'
            }).defaultValue = confirmHtml;

            form.addButton({
                id: 'custpage_new_import',
                label: 'Start New Import',
                functionName: 'window.location.reload()'
            });

            context.response.writePage(form);
        }

        /**
         * Show error page
         */
        function showError(context, errorMessage) {
            const form = serverWidget.createForm({
                title: 'BOM Import - Error'
            });

            const errorHtml = `
                <div style="padding: 20px; background-color: #f8d7da; border-radius: 5px; border-left: 4px solid #dc3545;">
                    <h2 style="margin-top: 0; color: #721c24;">Error</h2>
                    <p>${escapeHtml(errorMessage)}</p>
                </div>
            `;

            form.addField({
                id: 'custpage_error',
                type: serverWidget.FieldType.INLINEHTML,
                label: 'Error'
            }).defaultValue = errorHtml;

            form.addButton({
                id: 'custpage_back',
                label: 'Go Back',
                functionName: 'history.back()'
            });

            context.response.writePage(form);
        }

        /**
         * Parse CSV content
         */
        function parseCSV(content) {
            const lines = content.split('\n').filter(line => line.trim());
            
            if (lines.length === 0) {
                return { headers: [], rows: [] };
            }

            const headers = parseRow(lines[0]);
            const rows = [];

            for (let i = 1; i < lines.length; i++) {
                const row = parseRow(lines[i]);
                // Only include rows that have at least some data
                if (row.some(cell => cell.trim())) {
                    rows.push(row);
                }
            }

            return { headers, rows };
        }

        /**
         * Parse a single CSV row (handles quotes and commas)
         */
        function parseRow(rowText) {
            const values = [];
            let current = '';
            let inQuotes = false;

            for (let i = 0; i < rowText.length; i++) {
                const char = rowText[i];
                const nextChar = rowText[i + 1];

                if (char === '"') {
                    if (inQuotes && nextChar === '"') {
                        current += '"';
                        i++;
                    } else {
                        inQuotes = !inQuotes;
                    }
                } else if ((char === ',' || char === '\t') && !inQuotes) {
                    values.push(current.trim());
                    current = '';
                } else if (char !== '\r') {
                    current += char;
                }
            }
            values.push(current.trim());

            return values;
        }

        /**
         * Escape HTML to prevent XSS
         */
        function escapeHtml(text) {
            if (!text) return '';
            return String(text)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        return {
            onRequest: onRequest
        };
    });