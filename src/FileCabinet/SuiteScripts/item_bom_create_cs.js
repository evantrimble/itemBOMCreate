/**
 * @NApiVersion 2.1
 * @NScriptType ClientScript
 * @NModuleScope SameAccount
 */

/**
 * BOM Import Client Script
 *
 * Purpose: Provide UX toggle improvements for the mapping form (Step 2)
 *
 * Toggle Behaviors:
 * - Create Vendors checkbox: enables/disables vendor-related fields
 * - Setup MRP checkbox: enables/disables MRP fields and Item Location Defaults group
 * - Skip MRP Updates checkbox: only enabled when Setup MRP is checked
 * - Set MOQ checkbox: enables/disables MOQ percentage and values fields
 * - Custom field selection: enables/disables companion text input for custom field ID
 */

define(['N/currentRecord'], function(currentRecord) {

    /**
     * Page initialization - apply initial toggle states
     * @param {Object} context
     */
    function pageInit(context) {
        const rec = context.currentRecord;

        toggleVendorFields(rec);
        toggleMRPFields(rec);
        toggleSkipMRPField(rec);
        toggleMOQFields(rec);
        toggleAllCustomFields(rec);
    }

    /**
     * Field changed handler - respond to checkbox/select changes
     * @param {Object} context
     */
    function fieldChanged(context) {
        const rec = context.currentRecord;
        const fieldId = context.fieldId;

        if (fieldId === 'custpage_create_vendors') {
            toggleVendorFields(rec);
        }

        if (fieldId === 'custpage_setup_mrp') {
            toggleMRPFields(rec);
            toggleSkipMRPField(rec);
        }

        if (fieldId === 'custpage_set_moq') {
            toggleMOQFields(rec);
        }

        // Handle column mapping changes for custom field toggle
        if (fieldId.startsWith('custpage_map_col_')) {
            const colIndex = fieldId.replace('custpage_map_col_', '');
            toggleCustomField(rec, colIndex);
        }
    }

    /**
     * Toggle vendor-related fields based on Create Vendors checkbox
     * @param {Object} rec - current record
     */
    function toggleVendorFields(rec) {
        const createVendors = rec.getValue({ fieldId: 'custpage_create_vendors' });

        // Vendor Subsidiary: enabled when Create Vendors is checked
        const vendorSubField = rec.getField({ fieldId: 'custpage_vendor_subsidiary' });
        if (vendorSubField) {
            vendorSubField.isDisabled = !createVendors;
        }

        // Default Vendor ID: enabled when Create Vendors is unchecked
        const vendorIdField = rec.getField({ fieldId: 'custpage_vendor_id' });
        if (vendorIdField) {
            vendorIdField.isDisabled = createVendors;
        }
    }

    /**
     * Toggle MRP-related fields based on Setup MRP checkbox
     * @param {Object} rec - current record
     */
    function toggleMRPFields(rec) {
        const setupMRP = rec.getValue({ fieldId: 'custpage_setup_mrp' });

        // Item Location Defaults fields: enabled when Setup MRP is unchecked
        const locDefaultFields = [
            'custpage_pref_stock',
            'custpage_reorder_point',
            'custpage_safety_stock',
            'custpage_lead_time'
        ];

        locDefaultFields.forEach(function(fieldId) {
            const field = rec.getField({ fieldId: fieldId });
            if (field) {
                field.isDisabled = setupMRP;
            }
        });
    }

    /**
     * Toggle Skip MRP Updates field based on Setup MRP checkbox
     * @param {Object} rec - current record
     */
    function toggleSkipMRPField(rec) {
        const setupMRP = rec.getValue({ fieldId: 'custpage_setup_mrp' });

        const skipMrpField = rec.getField({ fieldId: 'custpage_skip_mrp_updates' });
        if (skipMrpField) {
            skipMrpField.isDisabled = !setupMRP;

            // If MRP is unchecked, also uncheck Skip MRP Updates
            if (!setupMRP) {
                rec.setValue({ fieldId: 'custpage_skip_mrp_updates', value: false });
            }
        }
    }

    /**
     * Toggle MOQ-related fields based on Set MOQ checkbox
     * @param {Object} rec - current record
     */
    function toggleMOQFields(rec) {
        const setMOQ = rec.getValue({ fieldId: 'custpage_set_moq' });

        const moqFields = [
            'custpage_moq_percent',
            'custpage_moq_values'
        ];

        moqFields.forEach(function(fieldId) {
            const field = rec.getField({ fieldId: fieldId });
            if (field) {
                field.isDisabled = !setMOQ;
            }
        });
    }

    /**
     * Toggle all custom field text inputs based on their dropdown selections
     * @param {Object} rec - current record
     */
    function toggleAllCustomFields(rec) {
        // Get column count from hidden field
        const columnCount = rec.getValue({ fieldId: 'custpage_column_count' }) || 0;

        for (let i = 0; i < columnCount; i++) {
            toggleCustomField(rec, i);
        }
    }

    /**
     * Toggle a single custom field text input based on its dropdown selection
     * @param {Object} rec - current record
     * @param {number|string} colIndex - column index
     */
    function toggleCustomField(rec, colIndex) {
        const selectValue = rec.getValue({ fieldId: 'custpage_map_col_' + colIndex });
        const customField = rec.getField({ fieldId: 'custpage_custom_field_' + colIndex });

        if (customField) {
            // Enable custom field input only when "_custom_" is selected
            customField.isDisabled = (selectValue !== '_custom_');
        }
    }

    return {
        pageInit: pageInit,
        fieldChanged: fieldChanged
    };
});
