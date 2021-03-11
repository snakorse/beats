#!/bin/bash -e

if [[ $DICE_IS_EDGE == 'true' ]]; then
    export OUTPUT_TERMINUS_HOSTS=$COLLECTOR_PUBLIC_URL
else
    export OUTPUT_TERMINUS_HOSTS=http://$COLLECTOR_ADDR
fi
echo $OUTPUT_TERMINUS_HOSTS

# migrate registry file
version_num=$(cat ./VERSION)
home_registry='/data/spot/filebeat/registry'
dst_registry=$home_registry'/log_tail'
meta_json_file=$home_registry'/filebeat/meta.json'

if [ "$version_num" == '4.0.0' ]; then
	if [ -f "$meta_json_file" ]; then
		echo "mv meta.json together with data.json"
	  cp $meta_json_file $dst_registry
	fi
fi

cfg_path='conf/filebeat.yml'
if [ "$FILEBEAT_CONFIG_PATH" != '' ]; then
	cfg_path=$FILEBEAT_CONFIG_PATH
fi

./filebeat -c "$cfg_path" -e
