import pandas as pd
from sqlalchemy import create_engine
from os import walk
import psycopg2.extras
import psycopg2

engine = create_engine('postgresql://postgres:password@localhost:5432/aireon')
year = '2023'
month = '04'

day_list = ['10','11','12','13','14','15']

for day in day_list:
    path = f"/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Aireon Data/TOMRDate={year}-{month}-{day}/"


    filenames = next(walk(path), (None, None, []))[2]

    df = pd.read_csv(path + filenames[0])

    for filename in filenames[1:]:
        df_temp = pd.read_csv(path + filename)
        df = pd.concat([df,df_temp])

    #print(df)

    #df.drop(['Mode 5 Data reports and Extended Mode 1 Code.Mode 5 PIN /National Origin/ Mission Code.Spare'], axis = 1)

    selected_column_list = ['FRN8AOSTcasResAdvisoryActive',
    'FRN8AOSTargetTrajectryChgRptCapability',
    'FRN8AOSTargetStateRptCapability',
    'FRN8AOSAirRefVelocityRptCapability',
    'FRN8AOSCockpitDisplayTrafficStatus',
    'FRN8AOSTcasSystemStatus',
    'FRN8AOSSingleAntenna',
    'FRN10DSISysAreaCode',
    'FRN10DSISysIdentCode',
    'FRN15SISvcIdent',
    'FRN16SMReptPrd',
    'FRN20ECEmitterCatCode',
    'FRN40TRDAddrTypeCode',
    'FRN40TRDAltRptCapability',
    'FRN40TRDRangeCheck',
    'FRN40TRDRptTypeCode',
    'FRN40TRDExt1DiffCorrection',
    'FRN40TRDExt1GroundBitSet',
    'FRN40TRDExt1SimTrgtMessage',
    'FRN40TRDExt1TestTarget',
    'FRN40TRDExt1SelAltAvailable',
    'FRN40TRDExt1ConfidenceLevel',
    'FRN40TRDExt2ListLookupCheckFailed',
    'FRN40TRDExt2IndPositionCheck',
    'FRN40TRDExt2NoGoBitStatus',
    'FRN40TRDExt2CompactPositionReport',
    'FRN40TRDExt2LocalDecodePosJump',
    'FRN40TRDExt2RangeCheck',
    'FRN70M3AMode3ACode',
    'FRN71TAPDateTimeOfAppltyForPos',
    'FRN72TAVDateTimeOfAppltyForVlcty',
    'FRN73TMRPDateTimeOfMessageRec',
    'FRN74TMRPHPFullSecondIndicator',
    'FRN74TMRPHPositionReceived',
    'FRN75TMRVDateTimeOfMsgRecForVel',
    'FRN76TMRVHPFullSecondIndicator',
    'FRN76TMRVelocityHighPrecision',
    'FRN77TARTDateTimeOfTransmission',
    'FRN80TATargetAddress',
    'FRN90QIVelctyNavCategory',
    'FRN90QPosNavCategory',
    'FRN90QIExt1NavIntgrtyCatBaroAlt',
    'FRN90QIExt1SrvlncSrcIntgrtyLvl',
    'FRN90QIExt1NavAccrcyCatCode',
    'FRN90QIExt2SilSupplement',
    'FRN90QIExt2HrztlPosSysDsgnAssurance',
    'FRN90QIExt2GeoAltitudeAccuracy',
    'FRN90QIExt3PosIntgrtyCategory',
    'FRN110TITrjctryIntentStatusProvided',
    'FRN110TITrjctryIntentDataProvided',
    'FRN110TISF1IntentDataAvailable',
    'FRN110TISF1DataValidIndicator',
    'FRN110TISF2RepetitionFactor',
    'FRN110TISF2TjctryChngPntNumAvailable',
    'FRN110TISF2TjctryChngPntNumComp',
    'FRN110TISF2ChangePointNumber',
    'FRN110TISF2Altitude',
    'FRN110TISF2Latitude',
    'FRN110TISF2Longitude',
    'FRN110TISF2PointTypeCode',
    'FRN110TISF2TurnDirectionCode',
    'FRN110TISF2TurnRadiusAvailable',
    'FRN110TISF2TovAvailable',
    'FRN110TISF2TimeOverPoint',
    'FRN110TISF2TrjctryChngPntTurnRadius',
    'FRN130PWCFloatingPointLat',
    'FRN130PWCFloatingPointLong',
    'FRN131HRPWCFloatingPointLat',
    'FRN131HRPWCFloatingPointLong',
    'FRN132MAMsgAmplitude',
    'FRN140GHGeometricHeight',
    'FRN145FLFlightLevel',
    'FRN146SASourceAvailable',
    'FRN146SASource',
    'FRN146SASelectedAltitude',
    'FRN148FSSAManageVerticalMode',
    'FRN148FSSAAltHoldMode',
    'FRN148FSSAApproachMode',
    'FRN148FSSASelectedAltitude',
    'FRN150ASAirSpeedType',
    'FRN150ASAirSpeed',
    'FRN151TASRangeExceeded',
    'FRN151TASTrueAirSpeed',
    'FRN152MHMagneticHeading',
    'FRN155BVRRangeExceeded',
    'FRN155BVRVerticalRate',
    'FRN157GVRRangeExceeded',
    'FRN157GVRVerticalRate',
    'FRN160AGVRangeExceeded',
    'FRN160AGVGroundSpeed',
    'FRN160AGVTrackAngle',
    'FRN161TNTrackNumber',
    'FRN165TARTrackAngleRatePerSec',
    'FRN170TITargetId',
    'FRN200TSIntentChange',
    'FRN200TSLateralNavMode',
    'FRN200TSMilitatryEmergency',
    'FRN200TSPriorityStatus',
    'FRN200TSurviellanceStatus',
    'FRN210MVVersionNotSupported',
    'FRN210MVMopsVersion',
    'FRN210MVLinktechTypeCode',
    'FRN220MIWindSpeedAvailable',
    'FRN220MIWindDirectionAvailable',
    'FRN220MITemperatureAvailable',
    'FRN220MITurbulenceAvailable',
    'FRN220MISF1WindSpeed',
    'FRN220MISF2WindDirection',
    'FRN220MISF3Tempurature',
    'FRN220MISF4Turbulence',
    'FRN230RARollAngle',
    'FRN250MSRepititionFactor',
    'FRN250MSCommBMessageId',
    'FRN250MSCommBDataBufferStore1Addr',
    'FRN250MSCommBDataBufferStore2Addr',
    'FRN260ARARTransponderMsgType',
    'FRN260ARARTransponderMsgSubType',
    'FRN260ARARActiveResAdvisory',
    'FRN260ARARActiveResAdvisoryCmplmnt',
    'FRN260ARARResolutionAdvisoryTerm',
    'FRN260ARARMultipleThreatEnctounter',
    'FRN260ARARThreatType',
    'FRN260ARARThreatIdentity',
    'FRN271SCCPositionOffsetApplied',
    'FRN271SCCCdtiSurfaceOperational',
    'FRN271SCCTransmitPowerLow',
    'FRN271SCCReceivingAtcService',
    'FRN271SCCIdentSwitchActive',
    'FRN271SCCExtAircraftLengthWidthCat',
    'FRN295DAAircraftOpStatusAgeAvailable',
    'FRN295DATargetReptDescAgeAvailable',
    'FRN295DAMode3CodeAgeAvailable',
    'FRN295DAQualtyIndictrAgeAvailable',
    'FRN295DATrajectoryIntentAgeAvailable',
    'FRN295DAMsgAmpAgeAvailable',
    'FRN295DAGeomHeightAgeAvailable',
    'FRN295DAFlightLevelAgeAvailable',
    'FRN295DAIntermedStateSelAltAgeAvailable',
    'FRN295DAFinalStateSelAltAgeAvailable',
    'FRN295DAAirSpeedAgeAvailable',
    'FRN295DATrueAirSpeedAgeAvailable',
    'FRN295DAMagneticHeadingAgeAvailable',
    'FRN295DABaroVertRateAgeAvailable',
    'FRN295DAGeoVertRateAgeAvailable',
    'FRN295DAGroundVectAgeAvailable',
    'FRN295DATrackAngleRateAgeAvailable',
    'FRN295DATargetIdAgeAvailable',
    'FRN295DATargetStatusAgeAvailable',
    'FRN295DAMetInfoAgeAvailable',
    'FRN295DARollAngleAgeAvailable',
    'FRN295DAAcasResAdvisryAgeAvailable',
    'FRN295DASurfaceCapabilityAgeAvailable',
    'FRN295DAAircraftOpStatusAge',
    'FRN295DATargetReportDescAge',
    'FRN295DASecondSurvMode3ACodeAge',
    'FRN295DAQualityIndicatorsAge',
    'FRN295DATrajectoryIntentAge',
    'FRN295DAMessageAmplitudeAge',
    'FRN295DAGeometricHeightAge',
    'FRN295DAFlightLevelAge',
    'FRN295DAIntermedStateSelAltAge',
    'FRN295DAFinalStateSelAltAge',
    'FRN295DAAirSpeedAge',
    'FRN295DATrueAirSpeedAge',
    'FRN295DAMagneticHeadingAge',
    'FRN295DABaromVerticalRateAge',
    'FRN295DAGeomVertRateAge',
    'FRN295DAGroundVectorAge',
    'FRN295DATrackAngleRateAge',
    'FRN295DATargetIdAge',
    'FRN295DATargetStatusAge',
    'FRN295DAMETInformationAge',
    'FRN295DARollAngleAge',
    'FRN295DAAcasResolutionAdvisoryAge',
    'FRN295DASurfaceCapabilityAge',
    'FRN400RISatelliteReceiverId',
    'REFieldLength',
    'REBPSBaronetricPressureSettingAvailable',
    'REBPSBarometricPressureSetting',
    'RESHPresenceAvailable',
    'RESHHorizontalReferanceDirection',
    'RESHStatusAvailable',
    'RESHSelectedHeading',
    'RENMPresenceAvailable',
    'RENMAutoPilotAvailable',
    'RENMVerticalNavigationAvailable',
    'RENMAltitudeHoldEngagedAvailable',
    'RENMApproachModeAvailable',
    'REGAOAntennaOffsetAvailable',
    'REGAOAntennaOffset',
    'RESGVGroundVectorAvailable',
    'RESGVAircraftStoppedAvailable',
    'RESGVTrackDataValidAvailable',
    'RESGVHeadingDataProvidedAvailable',
    'RESGVNorthCodeAvailable',
    'RESGVGroundSpeed',
    'RESGVExtendedHeadingGroundTrackInfo',
    'REASIStatusInformationAvailable',
    'REASITarget1090CapabilityAvailable',
    'REASITargetUatCapabilityAvailable',
    'RETNHPresenceAvailable',
    'RETNHTrueNorthHeading',
    'REMESPresenceAvailable',
    'REMESMode5SummaryAvailable',
    'REMESMode5IInterrogationStatus',
    'REMESMode5AuthReplyReportStatus',
    'REMESMode5AuthDataReplyReportStatus',
    'REMESMode5Mode1CodeStatus',
    'REMESMode5Mode2CodeStatus',
    'REMESMode5Mode3CodeStatus',
    'REMESMode5FlightLevelStatus',
    'REMESMode5SummaryPositionAvailable',
    'REMESMode5NationalOriginAvailable',
    'REMESMode5NaionaltOrginCode',
    'REMESMode5NationalOrginCodeValue',
    'REMESMode1PresenceAvailable',
    'REMESMode1Validated',
    'REMESMode1CodeIndicator',
    'REMESMode1Code',
    'REMESXPLSAvailable',
    'REMESXPLSMode5PinReplyReportAvailable',
    'REMESXPLSMode5DataReplyReportAvailable',
    'REMESXPLSModeCReplyAvailable',
    'REMESXPLSMode3AReplyAvailable',
    'REMESXPLSMode2ReplyAvailable',
    'REMESXPLSMode1ReplyAvailable',
    'REMESFigureOfMeritAvailable',
    'REMESFigureOfMerit',
    'REMESMode2Presence',
    'REMESMode2Validated',
    'REMESMode2CodeAvailable',
    'REMESMode2Code',
    'SPFMessageLength',
    'SPFMessageId',
    'SPFHostIdentifier',
    'FIRVolumeTags']


    print(selected_column_list)

    df_new = df[selected_column_list]

    print(df_new)

    table_name = f"aireon_2023_04_{day}"

    # Try to connect to the local PostGresSQL database in which we will store our flight trajectories coupled with FPL data.
    conn_postgres = psycopg2.connect(user="postgres",
                                     password="password",
                                     host="127.0.0.1",
                                     port="5432",
                                     database="aireon")
    with conn_postgres:
        cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgres_sql_text = "DROP TABLE IF EXISTS " + table_name + ";"
        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()

        df_new.to_sql(table_name, engine)

        postgres_sql_text = "drop table if exists \"" + table_name + "_geom\";" \
                "SELECT \"FRN71TAPDateTimeOfAppltyForPos\"," \
                "\"FRN170TITargetId\", " \
                "\"FRN80TATargetAddress\", " \
                "\"FRN145FLFlightLevel\", " \
                "\"FRN140GHGeometricHeight\", " \
                "\"FRN146SASelectedAltitude\", " \
                "\"FRN148FSSASelectedAltitude\", " \
                "\"FRN155BVRVerticalRate\", " \
                "\"FRN151TASTrueAirSpeed\", " \
                "\"FRN160AGVGroundSpeed\", " \
                "\"FRN131HRPWCFloatingPointLat\", " \
                "\"FRN131HRPWCFloatingPointLong\", " \
                "ST_SetSRID(ST_MakePoint(\"FRN131HRPWCFloatingPointLong\", " \
                "\"FRN131HRPWCFloatingPointLat\"),4326)" \
                " AS aircraft_derived_geom " \
                " INTO " + table_name + "_geom from \"" + table_name + "\";"

        cursor_postgres.execute(postgres_sql_text)
        conn_postgres.commit()