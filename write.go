/*
Copyright 2020 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package promrw

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func Parse(requestBody []byte, samplesHandler func(model.Samples) error) error {

	// decompress the body
	decompressedBody, err := snappy.Decode(nil, requestBody)
	if err != nil {
		return err
	}

	// decode the protobuf
	var promWriteRequest prompb.WriteRequest
	if err := proto.Unmarshal(decompressedBody, &promWriteRequest); err != nil {
		return err
	}

	// convert the protobuf to samples
	samples := promWriteRequestToSamples(&promWriteRequest)

	// call the handler
	return samplesHandler(samples)
}

func promWriteRequestToSamples(promWriteRequest *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range promWriteRequest.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}

	return samples
}
