/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */

/**
 * BOM Import Map/Reduce Script v2
 * 
 * Purpose: Import items, BOMs, and BOM revisions from CSV file using hierarchy notation
 * 
 * Hierarchy Notation:
 * - 1.0 = Top-level assembly
 * - 1.1, 1.2, 1.3 = Direct children of 1.0
 * - 1.2.1, 1.2.2 = Children of 1.2 (making 1.2 a sub-assembly)
 * 
 * Type Determination:
 * - If a hierarchy value is a prefix of any other row's hierarchy, it's an assembly
 * - Otherwise, it's an inventory item
 * 
 * Script Parameters:
 * - custscript_bom_config_file_id: Internal ID of the JSON config file
 * 
 * Processing Order:
 * 1. GET INPUT: Parse CSV and config, determine types from hierarchy
 * 2. MAP: Emit items with processing order keys
 * 3. REDUCE: Create items (inventory first, then assemblies)
 * 4. SUMMARIZE: Create BOMs and BOM Revisions
 */

define(['N/record', 'N/search', 'N/file', 'N/runtime', 'N/cache', 'N/format'],
    function(record, search, file, runtime, cache, format) {

        // Configuration constants
        const LOCATION_IDS = [2, 13];
        const VENDOR_ID = 625; // CDW
        const TAX_SCHEDULE_ID = 1;
        const ITEM_LOCATION_DEFAULTS = {
            preferredstocklevel: 1000,
            reorderpoint: 600,
            safetystocklevel: 100,
            leadtime: 7
        };

        /**
         * GET INPUT DATA
         */
        function getInputData() {
            try {
                log.audit('========================================', '');
                log.audit('GET INPUT DATA - Starting', '');
                log.audit('========================================', '');

                // Load config file
                const scriptObj = runtime.getCurrentScript();
                const configFileId = scriptObj.getParameter({ name: 'custscript_bom_config_file_id' });

                if (!configFileId) {
                    throw new Error('Config file ID parameter is required');
                }

                log.audit('Loading Config', 'Config File ID: ' + configFileId);

                const configFile = file.load({ id: configFileId });
                const config = JSON.parse(configFile.getContents());

                log.audit('Config Loaded', JSON.stringify({
                    prospectName: config.prospectName,
                    csvFileId: config.csvFileId,
                    mappings: config.mappings
                }));

                // Load CSV file
                const csvFile = file.load({ id: config.csvFileId });
                const csvContent = csvFile.getContents();
                const lines = csvContent.split('\n').filter(line => line.trim());

                if (lines.length < 2) {
                    throw new Error('CSV must have at least a header row and one data row');
                }

                // Parse headers (first row)
                const headers = parseRow(lines[0]);

                // Parse data rows
                const allRows = [];
                for (let i = 1; i < lines.length; i++) {
                    const rowData = parseRow(lines[i]);
                    if (rowData.some(cell => cell.trim())) {
                        allRows.push({
                            rowNumber: i + 1,
                            cells: rowData
                        });
                    }
                }

                log.audit('CSV Parsed', 'Headers: ' + headers.length + ', Data Rows: ' + allRows.length);

                // Map rows to fields using config
                const mappedRows = allRows.map(row => {
                    const mapped = {
                        rowNumber: row.rowNumber,
                        hierarchy: null,
                        itemFields: {},
                        bomFields: {}
                    };

                    Object.keys(config.mappings).forEach(colIndex => {
                        const fieldName = config.mappings[colIndex];
                        const value = row.cells[parseInt(colIndex)] || '';

                        if (!value.trim()) return;

                        if (fieldName === 'hierarchy') {
                            mapped.hierarchy = value.trim();
                        } else if (fieldName === 'quantity') {
                            mapped.bomFields.quantity = parseFloat(value) || 1;
                        } else if (fieldName === 'memo') {
                            mapped.bomFields.memo = value.trim();
                        } else {
                            // Item field
                            mapped.itemFields[fieldName] = value.trim();
                        }
                    });

                    return mapped;
                }).filter(row => row.hierarchy && row.itemFields.itemid);

                log.audit('Rows Mapped', mappedRows.length + ' valid rows with hierarchy and itemid');

                // Determine which rows are assemblies based on hierarchy
                const hierarchySet = new Set(mappedRows.map(r => r.hierarchy));
                
                mappedRows.forEach(row => {
                    // Check if any other hierarchy starts with this one + "."
                    const isAssembly = Array.from(hierarchySet).some(h => 
                        h !== row.hierarchy && h.startsWith(row.hierarchy + '.')
                    );
                    row.isAssembly = isAssembly;
                    row.recordType = isAssembly ? 'assemblyitem' : 'inventoryitem';

                    // Determine parent hierarchy
                    const parts = row.hierarchy.split('.');
                    if (parts.length > 1) {
                        parts.pop();
                        row.parentHierarchy = parts.join('.');
                    } else {
                        row.parentHierarchy = null; // Top-level item
                    }
                });

                // Log type breakdown
                const inventoryCount = mappedRows.filter(r => !r.isAssembly).length;
                const assemblyCount = mappedRows.filter(r => r.isAssembly).length;
                log.audit('Type Breakdown', 'Inventory: ' + inventoryCount + ', Assembly: ' + assemblyCount);

                // Store config in cache for summarize stage
                const importCache = cache.getCache({
                    name: 'BOM_IMPORT_CACHE',
                    scope: cache.Scope.PRIVATE
                });

                importCache.put({
                    key: 'IMPORT_CONFIG',
                    value: JSON.stringify({
                        prospectName: config.prospectName,
                        configFileId: configFileId,
                        csvFileId: config.csvFileId
                    }),
                    ttl: 7200
                });

                // Store all rows for summarize stage (for BOM creation)
                importCache.put({
                    key: 'ALL_ROWS',
                    value: JSON.stringify(mappedRows),
                    ttl: 7200
                });

                // Add prospect name to each row
                mappedRows.forEach(row => {
                    row.prospectName = config.prospectName;
                });

                log.audit('GET INPUT DATA - Complete', 'Returning ' + mappedRows.length + ' rows');

                return mappedRows;

            } catch (e) {
                log.error('getInputData Error', e.toString() + '\n' + e.stack);
                throw e;
            }
        }

        /**
         * Parse CSV row
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
                    values.push(current);
                    current = '';
                } else if (char !== '\r') {
                    current += char;
                }
            }
            values.push(current);

            return values;
        }

        /**
         * MAP - Classify and emit records
         */
        function map(context) {
            try {
                const rowData = JSON.parse(context.value);

                // Emit with keys that ensure processing order
                // Inventory items first, then assemblies
                if (rowData.isAssembly) {
                    context.write('2_assembly', context.value);
                } else {
                    context.write('1_inventory', context.value);
                }

            } catch (e) {
                log.error('map Error', 'Key: ' + context.key + ', Error: ' + e.toString());
            }
        }

        /**
         * REDUCE - Create items
         */
        function reduce(context) {
            try {
                const stage = context.key;
                const records = context.values.map(v => JSON.parse(v));

                log.audit('========================================', '');
                log.audit('REDUCE - ' + stage, 'Processing ' + records.length + ' records');
                log.audit('========================================', '');

                const importCache = cache.getCache({
                    name: 'BOM_IMPORT_CACHE',
                    scope: cache.Scope.PRIVATE
                });

                let created = 0;
                let skipped = 0;
                let failed = 0;

                records.forEach(rowData => {
                    try {
                        const externalId = rowData.prospectName + '_' + rowData.itemFields.itemid;

                        // Check if item already exists
                        const existingItem = findExistingItem(externalId);

                        if (existingItem) {
                            // Item exists, just cache the ID
                            log.debug('Item Exists', 'Item: ' + rowData.itemFields.itemid + ' (ID: ' + existingItem + ')');
                            
                            const cacheKey = rowData.prospectName + '_' + rowData.itemFields.itemid;
                            importCache.put({ key: cacheKey, value: existingItem.toString(), ttl: 7200 });
                            
                            skipped++;
                        } else {
                            // Create new item
                            const itemId = createItem(rowData);

                            if (itemId) {
                                const cacheKey = rowData.prospectName + '_' + rowData.itemFields.itemid;
                                importCache.put({ key: cacheKey, value: itemId.toString(), ttl: 7200 });
                                
                                log.audit('Item Created', rowData.recordType + ': ' + rowData.itemFields.itemid + ' (ID: ' + itemId + ')');
                                created++;
                            } else {
                                failed++;
                            }
                        }
                    } catch (e) {
                        failed++;
                        log.error('Item Failed', 'Row ' + rowData.rowNumber + ' (' + rowData.itemFields.itemid + '): ' + e.toString());
                    }
                });

                log.audit('REDUCE Complete', stage + ' - Created: ' + created + ', Skipped: ' + skipped + ', Failed: ' + failed);

            } catch (e) {
                log.error('reduce Error', 'Stage: ' + context.key + ', Error: ' + e.toString() + '\n' + e.stack);
            }
        }

        /**
         * Find existing item by external ID
         */
        function findExistingItem(externalId) {
            try {
                const itemSearch = search.create({
                    type: 'item',
                    filters: [['externalid', 'is', externalId]],
                    columns: ['internalid']
                });

                const results = itemSearch.run().getRange({ start: 0, end: 1 });
                
                if (results.length > 0) {
                    return results[0].getValue('internalid');
                }
            } catch (e) {
                log.debug('Item Search Error', e.toString());
            }
            return null;
        }

        /**
         * Create Item (Inventory or Assembly)
         */
        function createItem(rowData) {
            const itemRec = record.create({
                type: rowData.recordType,
                isDynamic: true
            });

            // Set External ID
            const externalId = rowData.prospectName + '_' + rowData.itemFields.itemid;
            itemRec.setValue({ fieldId: 'externalid', value: externalId });

            // Set mapped fields
            Object.keys(rowData.itemFields).forEach(field => {
                try {
                    const value = rowData.itemFields[field];
                    if (value) {
                        itemRec.setValue({ fieldId: field, value: value });
                    }
                } catch (e) {
                    log.debug('Field Set Warning', 'Field: ' + field + ', Error: ' + e.toString());
                }
            });

            // Set standard fields
            itemRec.setValue({ fieldId: 'includechildren', value: true });
            itemRec.setValue({ fieldId: 'taxschedule', value: TAX_SCHEDULE_ID });

            try {
                itemRec.setValue({ fieldId: 'replenishmentmethod', value: 'REORDER_POINT' });
                itemRec.setValue({ fieldId: 'autoreorderpoint', value: false });
                itemRec.setValue({ fieldId: 'autoleadtime', value: false });
                itemRec.setValue({ fieldId: 'autopreferredstocklevel', value: false });
            } catch (e) {
                log.debug('Planning Fields Warning', e.toString());
            }

            // Add CDW as vendor (for inventory items)
            if (rowData.recordType === 'inventoryitem') {
                try {
                    itemRec.selectNewLine({ sublistId: 'itemvendor' });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'itemvendor',
                        fieldId: 'vendor',
                        value: VENDOR_ID
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'itemvendor',
                        fieldId: 'preferredvendor',
                        value: true
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'itemvendor',
                        fieldId: 'purchaseprice',
                        value: 1
                    });
                    itemRec.commitLine({ sublistId: 'itemvendor' });
                } catch (e) {
                    log.debug('Vendor Warning', e.toString());
                }
            }

            const itemId = itemRec.save();

            // Create item locations
            createItemLocations(itemId, rowData.recordType);

            return itemId;
        }

        /**
         * Create Item Locations
         */
        function createItemLocations(itemId, recordType) {
            LOCATION_IDS.forEach(locationId => {
                try {
                    const itemRec = record.load({
                        type: recordType,
                        id: itemId,
                        isDynamic: true
                    });

                    itemRec.selectNewLine({ sublistId: 'locations' });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'locations',
                        fieldId: 'location',
                        value: locationId
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'locations',
                        fieldId: 'preferredstocklevel',
                        value: ITEM_LOCATION_DEFAULTS.preferredstocklevel
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'locations',
                        fieldId: 'reorderpoint',
                        value: ITEM_LOCATION_DEFAULTS.reorderpoint
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'locations',
                        fieldId: 'safetystocklevel',
                        value: ITEM_LOCATION_DEFAULTS.safetystocklevel
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'locations',
                        fieldId: 'leadtime',
                        value: ITEM_LOCATION_DEFAULTS.leadtime
                    });
                    itemRec.commitLine({ sublistId: 'locations' });

                    itemRec.save();
                } catch (e) {
                    log.debug('Item Location Warning', 'Item: ' + itemId + ', Location: ' + locationId + ', Error: ' + e.toString());
                }
            });
        }

        /**
         * SUMMARIZE - Create BOMs and BOM Revisions
         */
        function summarize(context) {
            try {
                log.audit('========================================', '');
                log.audit('SUMMARIZE - Creating BOMs', '');
                log.audit('========================================', '');

                const importCache = cache.getCache({
                    name: 'BOM_IMPORT_CACHE',
                    scope: cache.Scope.PRIVATE
                });

                // Get config from cache
                let config;
                try {
                    const configStr = importCache.get({ key: 'IMPORT_CONFIG' });
                    config = JSON.parse(configStr);
                } catch (e) {
                    log.error('Config Cache Miss', 'Attempting to reload from script parameter');
                    
                    // Fallback: reload from script parameter
                    const scriptObj = runtime.getCurrentScript();
                    const configFileId = scriptObj.getParameter({ name: 'custscript_bom_config_file_id' });
                    const configFile = file.load({ id: configFileId });
                    const fullConfig = JSON.parse(configFile.getContents());
                    config = {
                        prospectName: fullConfig.prospectName,
                        csvFileId: fullConfig.csvFileId
                    };
                }

                // Get all rows from cache
                let allRows;
                try {
                    const rowsStr = importCache.get({ key: 'ALL_ROWS' });
                    allRows = JSON.parse(rowsStr);
                } catch (e) {
                    log.error('Rows Cache Miss', 'Cannot create BOMs without row data');
                    return;
                }

                const prospectName = config.prospectName;

                // Find all assemblies
                const assemblies = allRows.filter(row => row.isAssembly);
                
                log.audit('Assemblies to Process', assemblies.length);

                let bomsCreated = 0;
                let bomsFailed = 0;
                let revisionsCreated = 0;
                let revisionsFailed = 0;

                assemblies.forEach(assembly => {
                    try {
                        const assemblyItemId = assembly.itemFields.itemid;
                        const assemblyHierarchy = assembly.hierarchy;

                        // Get assembly internal ID from cache
                        const cacheKey = prospectName + '_' + assemblyItemId;
                        const assemblyInternalId = importCache.get({ key: cacheKey });

                        if (!assemblyInternalId) {
                            log.error('Assembly Not Cached', 'Assembly ' + assemblyItemId + ' not found in cache');
                            bomsFailed++;
                            return;
                        }

                        // Find direct children (one level deeper)
                        const directChildren = allRows.filter(row => {
                            if (row.parentHierarchy === assemblyHierarchy) {
                                return true;
                            }
                            return false;
                        });

                        if (directChildren.length === 0) {
                            log.audit('No Children', 'Assembly ' + assemblyItemId + ' has no direct children - skipping BOM');
                            return;
                        }

                        // Build component list with internal IDs
                        const components = [];
                        directChildren.forEach(child => {
                            const childCacheKey = prospectName + '_' + child.itemFields.itemid;
                            const childInternalId = importCache.get({ key: childCacheKey });

                            if (childInternalId) {
                                components.push({
                                    itemId: child.itemFields.itemid,
                                    internalId: childInternalId,
                                    quantity: child.bomFields.quantity || 1
                                });
                            } else {
                                log.error('Component Not Cached', 'Component ' + child.itemFields.itemid + ' not found in cache');
                            }
                        });

                        if (components.length === 0) {
                            log.error('No Valid Components', 'Assembly ' + assemblyItemId + ' has no valid components');
                            bomsFailed++;
                            return;
                        }

                        // Create BOM
                        const bomId = createBOM(assemblyItemId, assemblyInternalId, prospectName);

                        if (bomId) {
                            bomsCreated++;

                            // Create BOM Revision with components
                            const revisionCreated = createBOMRevision(bomId, assemblyItemId, components, prospectName);
                            
                            if (revisionCreated) {
                                revisionsCreated++;
                            } else {
                                revisionsFailed++;
                            }
                        } else {
                            bomsFailed++;
                        }

                    } catch (e) {
                        bomsFailed++;
                        log.error('BOM Process Failed', 'Assembly: ' + assembly.itemFields.itemid + ', Error: ' + e.toString());
                    }
                });

                // Log summary
                log.audit('========================================', '');
                log.audit('IMPORT SUMMARY', '');
                log.audit('========================================', '');
                log.audit('Prospect', prospectName);
                log.audit('BOMs Created', bomsCreated);
                log.audit('BOMs Failed', bomsFailed);
                log.audit('Revisions Created', revisionsCreated);
                log.audit('Revisions Failed', revisionsFailed);

                // Log any Map/Reduce stage errors
                logStageErrors(context);

                log.audit('========================================', '');
                log.audit('IMPORT COMPLETE', '');
                log.audit('========================================', '');

            } catch (e) {
                log.error('summarize Error', e.toString() + '\n' + e.stack);
            }
        }

        /**
         * Create BOM
         */
        function createBOM(assemblyItemId, assemblyInternalId, prospectName) {
            try {
                const bomName = assemblyItemId + '_BOM';
                const externalId = prospectName + '_' + bomName;

                // Check if BOM already exists
                const existingBom = findExistingBOM(externalId);
                if (existingBom) {
                    log.debug('BOM Exists', 'BOM: ' + bomName + ' (ID: ' + existingBom + ')');
                    return existingBom;
                }

                const bomRec = record.create({
                    type: 'bom',
                    isDynamic: true
                });

                bomRec.setValue({ fieldId: 'name', value: bomName });
                bomRec.setValue({ fieldId: 'subsidiary', value: 1 });
                bomRec.setValue({ fieldId: 'includechildren', value: true });
                bomRec.setValue({ fieldId: 'externalid', value: externalId });

                const bomId = bomRec.save();

                log.audit('BOM Created', 'BOM: ' + bomName + ' (ID: ' + bomId + ')');

                return bomId;

            } catch (e) {
                log.error('BOM Creation Failed', 'Assembly: ' + assemblyItemId + ', Error: ' + e.toString());
                return null;
            }
        }

        /**
         * Find existing BOM by external ID
         */
        function findExistingBOM(externalId) {
            try {
                const bomSearch = search.create({
                    type: 'bom',
                    filters: [['externalid', 'is', externalId]],
                    columns: ['internalid']
                });

                const results = bomSearch.run().getRange({ start: 0, end: 1 });
                
                if (results.length > 0) {
                    return results[0].getValue('internalid');
                }
            } catch (e) {
                log.debug('BOM Search Error', e.toString());
            }
            return null;
        }

        /**
         * Create BOM Revision with components
         */
        function createBOMRevision(bomId, assemblyItemId, components, prospectName) {
            try {
                const revisionName = assemblyItemId + '_REV_A';
                const externalId = prospectName + '_' + revisionName;

                // Check if revision already exists
                const existingRevision = findExistingBOMRevision(externalId);
                if (existingRevision) {
                    log.debug('BOM Revision Exists', 'Revision: ' + revisionName + ' (ID: ' + existingRevision + ')');
                    return true;
                }

                const yesterday = new Date();
                yesterday.setDate(yesterday.getDate() - 1);

                const bomRevRec = record.create({
                    type: 'bomrevision',
                    isDynamic: true
                });

                bomRevRec.setValue({ fieldId: 'name', value: revisionName });
                bomRevRec.setValue({ fieldId: 'billofmaterials', value: bomId });
                bomRevRec.setValue({ fieldId: 'effectivestartdate', value: yesterday });
                bomRevRec.setValue({ fieldId: 'memo', value: 'Initial revision - ' + prospectName });
                bomRevRec.setValue({ fieldId: 'externalid', value: externalId });

                // Add components
                let componentsAdded = 0;
                components.forEach(component => {
                    try {
                        bomRevRec.selectNewLine({ sublistId: 'component' });
                        bomRevRec.setCurrentSublistValue({
                            sublistId: 'component',
                            fieldId: 'item',
                            value: component.internalId
                        });
                        bomRevRec.setCurrentSublistValue({
                            sublistId: 'component',
                            fieldId: 'quantity',
                            value: component.quantity
                        });
                        bomRevRec.commitLine({ sublistId: 'component' });
                        componentsAdded++;
                    } catch (e) {
                        log.error('Component Add Failed', 'Component: ' + component.itemId + ', Error: ' + e.toString());
                    }
                });

                const revisionId = bomRevRec.save();

                log.audit('BOM Revision Created', 'Revision: ' + revisionName + ' (ID: ' + revisionId + ') with ' + componentsAdded + ' components');

                return true;

            } catch (e) {
                log.error('BOM Revision Failed', 'Assembly: ' + assemblyItemId + ', Error: ' + e.toString());
                return false;
            }
        }

        /**
         * Find existing BOM Revision by external ID
         */
        function findExistingBOMRevision(externalId) {
            try {
                const revSearch = search.create({
                    type: 'bomrevision',
                    filters: [['externalid', 'is', externalId]],
                    columns: ['internalid']
                });

                const results = revSearch.run().getRange({ start: 0, end: 1 });
                
                if (results.length > 0) {
                    return results[0].getValue('internalid');
                }
            } catch (e) {
                log.debug('BOM Revision Search Error', e.toString());
            }
            return null;
        }

        /**
         * Log Map/Reduce stage errors
         */
        function logStageErrors(context) {
            if (context.inputSummary.error) {
                log.error('INPUT Error', context.inputSummary.error);
            }

            context.mapSummary.errors.iterator().each(function(key, error) {
                log.error('MAP Error', 'Key: ' + key + ', Error: ' + error);
                return true;
            });

            context.reduceSummary.errors.iterator().each(function(key, error) {
                log.error('REDUCE Error', 'Key: ' + key + ', Error: ' + error);
                return true;
            });
        }

        return {
            getInputData: getInputData,
            map: map,
            reduce: reduce,
            summarize: summarize
        };
    });