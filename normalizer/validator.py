# normalizer/validator.py

import logging
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
import json
from shared.models.common import StandardizedOutput, ValidatedOutput, ErrorMessage
from preservarium_sdk.infrastructure.sql_repository.sql_datatype_repository import SQLDatatypeRepository
from preservarium_sdk.infrastructure.sql_repository.sql_field_repository import SQLFieldRepository

logger = logging.getLogger(__name__)

class Validator:
    """
    Validates and normalizes sensor data based on configurable rules
    stored in the datatypes and datatypes_data tables.
    """
    
    def __init__(self, datatype_repository, field_repository):
        """
        Initialize the validator with repositories for accessing validation rules.
        """
        self.datatype_repository = datatype_repository
        self.field_repository = field_repository
        
    async def validate_and_normalize(
        self, 
        device_id: str,
        datatype_id: str,
        standardized_data: StandardizedOutput,
        request_id: Optional[str] = None,
        datatype: Optional[Any] = None
    ) -> Tuple[Optional[StandardizedOutput], List[ErrorMessage]]:
        """
        Validate and normalize data based on datatype configuration.
        
        Args:
            device_id: The ID of the device sending the data
            datatype_id: The ID of the datatype to validate against
            standardized_data: StandardizedOutput object from the parser script
            request_id: Optional request ID for tracing
            datatype: Optional datatype object (if already fetched)
            
        Returns:
            Tuple with validated StandardizedOutput (or None if validation failed)
            and a list of any validation errors
        """
        errors = []
        
        # 2. Get validation parameters from datatypes_data
        validation_params = await self._get_validation_params(datatype_id)
        # Values to be processed
        values = standardized_data.values
        
        # 3. Apply any parsing operations first
        parsing_applied = False
        
        parsing_valid, parsed_values, parsing_errors = self._apply_parsing_operations(
            values,
            validation_params
        )
        
        # Add any parsing operation errors
        for error_msg in parsing_errors:
            errors.append(ErrorMessage(
                error=error_msg,
                original_message={"device_id": device_id},
                request_id=request_id
            ))
        
        if parsing_valid and parsed_values:
            # Use the parsed values for further validation
            values = parsed_values
            parsing_applied = True
        logger.debug(f"parsing_applied: {parsing_applied}, parsed_values: {parsed_values}")
        # 4. Validate all values against the datatype
        type_valid, converted_values, type_errors = self._validate_type(
            values, 
            datatype.data_type
        )
        logger.debug(f"type_valid: {type_valid}, converted_values: {converted_values}, type_errors: {type_errors}")
        # Add any type validation errors
        for error_msg in type_errors:
            errors.append(ErrorMessage(
                error=error_msg,
                original_message={"device_id": device_id},
                request_id=request_id
            ))
        
        # 5. Validate range for all values
        range_valid, range_results, range_errors = self._validate_range(
            converted_values,
            validation_params
        )
        logger.debug(f"range_valid: {range_valid}, range_results: {range_results}, range_errors: {range_errors}")
        # Add any range validation errors
        for error_msg in range_errors:
            errors.append(ErrorMessage(
                error=error_msg,
                original_message={"device_id": device_id},
                request_id=request_id
            ))
        
        # 6. Build validated output if all validations pass
        if (type_valid and range_valid):
            # Apply any needed transformations
            validated_output = self._build_validated_output(
                device_id, 
                datatype,
                converted_values,
                request_id,
                standardized_data,
                validation_params
            )
            return validated_output, errors
        
        return None, errors
    
    async def _get_validation_params(self, datatype_id: str) -> Dict[str, Any]:
        """
        Fetches validation parameters from datatypes_data table for a specific datatype.
        Converts field values to a dictionary for easier access.
        """
        validation_params = {}
        try:
            # This method needs to be implemented in the datatype repository
            datatype_data_list = await self.datatype_repository.get_datatype_fields_data(datatype_id)
            
            for data_item in datatype_data_list:
                field_name = data_item.get("field_name")
                field_value = data_item.get("field_value")
                if field_name and field_value is not None:
                    validation_params[field_name] = field_value
                    
            logger.debug(f"Fetched validation parameters for datatype {datatype_id}: {validation_params}")
            return validation_params
        except Exception as e:
            logger.error(f"Error fetching validation parameters for datatype {datatype_id}: {e}")
            return {}
        
    def _validate_range(self, values: List[Any], validation_params: Dict) -> Tuple[bool, List[bool], List[str]]:
        """
        Check if values are within the range defined in validation parameters.
        
        Args:
            values: The list of values to validate
            validation_params: Dictionary of validation parameters
            
        Returns:
            Tuple of (all_valid, validation_results, error_messages)
        """
        if not values:
            return False, [], ["No values provided for range validation"]
        
        # Get min and max values from validation params
        min_value = validation_params.get("min_value")
        max_value = validation_params.get("max_value")
        
        # If no range is defined, consider all values valid
        if min_value is None or max_value is None:
            return True, [True] * len(values), []
        
        # Try to convert range bounds to float
        try:
            min_val = float(min_value)
            max_val = float(max_value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid range bounds: min={min_value}, max={max_value}")
            return False, [False] * len(values), [f"Invalid range bounds: min={min_value}, max={max_value}"]
        
        # Validate each value
        all_valid = True
        validation_results = []
        error_messages = []
        
        for i, value in enumerate(values):
            if value is None:
                validation_results.append(False)
                error_messages.append(f"Value at index {i} is None")
                all_valid = False
                continue
            
            try:
                value_float = float(value)
                valid = min_val <= value_float <= max_val
                validation_results.append(valid)
                
                if not valid:
                    error_messages.append(f"Value {value} at index {i} is outside allowed range [{min_val}, {max_val}]")
                    all_valid = False
            except (ValueError, TypeError):
                validation_results.append(False)
                error_messages.append(f"Range validation failed due to type conversion error: {value}")
                all_valid = False
        
        return all_valid, validation_results, error_messages
        
    def _validate_type(self, values: List[Any], expected_type: str) -> Tuple[bool, List[Any], List[str]]:
        """
        Validate and convert a list of values to the expected type if possible.
        
        Args:
            values: The list of values to validate and convert
            expected_type: The expected data type (string, float, integer, boolean)
            
        Returns:
            Tuple of (all_valid, converted_values, error_messages)
        """
        if not values:
            return False, [], ["No values provided for validation"]
        
        if not expected_type:
            # If no expected type is specified, consider any values valid
            return True, values, []
        
        converted_values = []
        error_messages = []
        all_valid = True
        
        for i, value in enumerate(values):
            if value is None:
                converted_values.append(None)
                error_messages.append(f"Value at index {i} is None")
                all_valid = False
                continue
            
            try:
                if expected_type.lower() == 'number':
                    converted_values.append(float(value))
                elif expected_type.lower() == 'integer':
                    converted_values.append(int(float(value)))
                elif expected_type.lower() == 'boolean':
                    if isinstance(value, bool):
                        converted_values.append(value)
                    elif isinstance(value, str):
                        converted_values.append(value.lower() in ('true', 'yes', '1'))
                    else:
                        converted_values.append(bool(value))
                elif expected_type.lower() == 'string':
                    converted_values.append(str(value))
                else:
                    # Default case - no conversion
                    logger.info(f"Using default validation for unrecognized type: {expected_type}")
                    converted_values.append(value)
            except (ValueError, TypeError) as e:
                logger.warning(f"Type validation failed: {value} is not a valid {expected_type}. Error: {e}")
                converted_values.append(value)  # Keep original value
                error_messages.append(f"Type validation failed for value '{value}': expected {expected_type}. Error: {e}")
                all_valid = False
        
        return all_valid, converted_values, error_messages
                
    def _build_validated_output(
        self, 
        device_id: str, 
        datatype: Any, 
        converted_values: List[Any], 
        request_id: Optional[str] = None,
        standardized_data: Optional[StandardizedOutput] = None,
        validation_params: Optional[Dict] = None
    ) -> ValidatedOutput:
        """
        Build a ValidatedOutput object from validated data.
        Also creates a ValidatedOutput object for internal validation tracking.
        
        Args:
            device_id: The device ID
            datatype: The datatype model
            converted_values: List of validated and converted values
            request_id: Optional request ID
            standardized_data: Optional StandardizedOutput object
        Returns:
            ValidatedOutput object with validated data
        """
        # Get unit label from datatype if available
        label = []
        if standardized_data.label:
            label = standardized_data.label
        elif datatype.label:
            label = json.loads(datatype.label)
        
        # Build metadata with validation info
        # Always create base metadata
        metadata = {
            "datatype_id": str(datatype.id) if hasattr(datatype, 'id') else None,
            "datatype_name": datatype.name if hasattr(datatype, 'name') else None,
            "datatype_unit": validation_params.get("unit", None),
            "persist": datatype.persist if hasattr(datatype, 'persist') else True,
        }
        
        # Conditionally merge standardized_data.metadata if it exists and has content
        if standardized_data.metadata and len(standardized_data.metadata) > 0:
            metadata.update(standardized_data.metadata)
            
        # Create ValidatedOutput for internal tracking (no metadata)
        validated_output = ValidatedOutput(
            device_id=device_id,
            values=converted_values,
            label=label,
            index=datatype.datatype_index if hasattr(datatype, 'datatype_index') else "",
            request_id=request_id,
            timestamp=standardized_data.timestamp,
            metadata=metadata
        )
        
        return validated_output

    def _apply_parsing_operations(self, values: List[Any], validation_params: Dict) -> Tuple[bool, List[Any], List[str]]:
        """
        Apply parsing operations to values based on the operation parameter.
        
        Args:
            values: List of values to apply operations to
            validation_params: Dictionary of validation parameters
            
        Returns:
            Tuple of (success, parsed_values, error_messages)
        """
        if not values:
            return True, [], []
        
        # Look for the operation parameter - it might be under 'operation' key
        parsing_op = validation_params.get("parsing_op")
        if not parsing_op:
            # No operation to apply
            return True, values, []
        
        # Initialize result variables
        success = True
        parsed_values = []
        error_messages = []
        
        try:
            # Process each value with the parsing operation formula
            for i, value in enumerate(values):
                if value is None:
                    parsed_values.append(None)
                    continue
                    
                try:
                    # Replace %val% in the formula with the actual value
                    formula = parsing_op.replace("%val%", str(value))
                    
                    # Evaluate the formula safely
                    # This handles operations like "(%val%/100)" or "(%val%-10000)/100"
                    result = eval(formula, {"__builtins__": {}})
                    parsed_values.append(result)
                    
                    logger.debug(f"Applied operation '{parsing_op}' to value '{value}' with result: {result}")
                    
                except (ValueError, TypeError, SyntaxError, NameError) as e:
                    error_messages.append(f"Failed to apply operation '{parsing_op}' to value '{value}': {str(e)}")
                    parsed_values.append(value)  # Keep original value
                    success = False
                    
        except Exception as e:
            # Handle unexpected errors
            error_messages.append(f"Error applying operation: {str(e)}")
            return False, values, error_messages
        
        return success, parsed_values, error_messages
