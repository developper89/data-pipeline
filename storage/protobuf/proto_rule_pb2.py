# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto_rule.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10proto_rule.proto\"S\n\rProtoCalendar\x12\x11\n\trule_mask\x18\x01 \x01(\r\x12\x12\n\nparameters\x18\x02 \x03(\x11\x12\x1b\n\x04type\x18\x03 \x01(\x0e\x32\r.CalendarType\"m\n\tProtoRule\x12\x14\n\x0c\x63hannel_mask\x18\x01 \x01(\r\x12\x1d\n\tcondition\x18\x02 \x01(\x0e\x32\n.Condition\x12\x12\n\nparameters\x18\x03 \x03(\x11\x12\x17\n\x06\x61\x63tion\x18\x04 \x01(\x0e\x32\x07.Action*\xf6\x02\n\tCondition\x12\x19\n\x15\x43ONDITION_UNSPECIFIED\x10\x00\x12\x16\n\x12\x43ONDITION_DISABLED\x10\x01\x12\x1c\n\x18\x43ONDITION_HIGH_THRESHOLD\x10\x02\x12\x1b\n\x17\x43ONDITION_LOW_THRESHOLD\x10\x03\x12\x1c\n\x18\x43ONDITION_DIFF_THRESHOLD\x10\x04\x12!\n\x1d\x43ONDITION_BINARY_CHANGE_STATE\x10\x05\x12\x1c\n\x18\x43ONDITION_LOGIC_OPERATOR\x10\x06\x12\x1c\n\x18\x43ONDITION_ON_MEASUREMENT\x10\x07\x12)\n%CONDITION_HIGH_THRESHOLD_DUAL_CHANNEL\x10\x08\x12(\n$CONDITION_LOW_THRESHOLD_DUAL_CHANNEL\x10\t\x12)\n%CONDITION_DIFF_THRESHOLD_DUAL_CHANNEL\x10\n*^\n\rLogicOperator\x12\x1e\n\x1aLOGIC_OPERATOR_UNSPECIFIED\x10\x00\x12\x16\n\x12LOGIC_OPERATOR_AND\x10\x01\x12\x15\n\x11LOGIC_OPERATOR_OR\x10\x02*\xa3\x01\n\x06\x41\x63tion\x12\x16\n\x12\x41\x43TION_UNSPECIFIED\x10\x00\x12\x1f\n\x1b\x41\x43TION_TRIGGER_TRANSMISSION\x10\x01\x12\x14\n\x10\x41\x43TION_NO_ACTION\x10\x02\x12(\n$ACTION_TRIGGER_TRANSMISSION_WITH_ACK\x10\x03\x12 \n\x1c\x41\x43TION_FAST_ADVERTISING_MODE\x10\x04*a\n\x0c\x43\x61lendarType\x12\x1d\n\x19\x43\x41LENDAR_TYPE_UNSPECIFIED\x10\x00\x12\x1a\n\x16\x43\x41LENDAR_TYPE_DISABLED\x10\x01\x12\x16\n\x12\x43\x41LENDAR_TYPE_WEEK\x10\x02\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto_rule_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _CONDITION._serialized_start=217
  _CONDITION._serialized_end=591
  _LOGICOPERATOR._serialized_start=593
  _LOGICOPERATOR._serialized_end=687
  _ACTION._serialized_start=690
  _ACTION._serialized_end=853
  _CALENDARTYPE._serialized_start=855
  _CALENDARTYPE._serialized_end=952
  _PROTOCALENDAR._serialized_start=20
  _PROTOCALENDAR._serialized_end=103
  _PROTORULE._serialized_start=105
  _PROTORULE._serialized_end=214
# @@protoc_insertion_point(module_scope)
